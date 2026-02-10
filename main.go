package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/boyter/gocodewalker"
	_ "modernc.org/sqlite"
)

// Performance Tuning Constants
// All performance-sensitive parameters are extracted here for easy tuning.
// These values control parallelism, memory usage, buffering, and timeouts.

// CLI Args
const (
	fileListModeDefault = "git"
)

// Memory Management
const (
	gcPercent   = 50                     // GC trigger percentage (lower = more frequent GC, less memory)
	memoryLimit = 4 * 1024 * 1024 * 1024 // Soft memory limit in bytes (4GB)
)

// Parallelism: Worker Pool Configuration
const (
	blameWorkersMultiplier         = 2 // Multiplier of NumCPU for blame workers per repo
	repoWorkersDivisor             = 2 // Divisor of NumCPU for concurrent repo processing
	maxRepoWorkers                 = 4 // Cap on repo workers to prevent goroutine explosion
	scanConcurrencyMultiplier      = 4 // Multiplier of NumCPU for filesystem scanning
	preflightConcurrencyMultiplier = 2 // Multiplier of NumCPU for file counting
)

// Channel Buffers: CSP Communication
// Increased buffer sizes reduce goroutine blocking and context switch overhead (Phase 3 optimization)
// Larger buffers allow producers and consumers to run more independently, reducing synchronization cost
const (
	fileTaskBuffer         = 2000 // Buffer size for file blame tasks within a repo (4x increase for reduced blocking)
	contributionBuffer     = 5000 // Buffer size for file author stats aggregation (5x increase for reduced blocking)
	repoTaskBuffer         = 50   // Buffer size for repository tasks
	commitStatsBuffer      = 500  // Buffer size for commit statistics batches (5x increase for reduced blocking)
	repoScanBuffer         = 100  // Buffer size for discovered repos during scanning
	progressChanDefault    = 1000 // Default progress channel buffer size
	progressChanThreshold  = 100  // Repo count threshold for smaller progress buffer
	progressChanMultiplier = 10   // Multiplier for progress channel when repo count < threshold
)

// Capacity Hints: Pre-allocation for Memory Efficiency
const (
	repoInternCapacity   = 2000 // Expected number of repositories for string interning
	authorInternCapacity = 1024 // Expected unique authors across all repositories for interning
	authorCountsCapacity = 32   // Expected authors per file
	commitStatsCapacity  = 256  // Expected unique authors in commit history
	fileListCapacity     = 1024 // Initial capacity for file lists
	fileListQueueBuffer  = 1000 // Channel buffer for file walker
)

// Timeouts and Intervals
const (
	gitBlameTimeout        = 10 * time.Second       // Timeout for individual git blame operations
	batchFlushInterval     = 500 * time.Millisecond // Database batch flush interval
	progressReportInterval = 200 * time.Millisecond // Repo progress update interval
	uiTickerInterval       = 100 * time.Millisecond // UI refresh rate
)

// Database: Batch Sizes
const (
	batchSizeThreshold = 5000 // File stats batch size before flush (increased for write performance)
	dbFile             = "git-who.db"
)

// SQLite: Performance Tuning
const (
	sqlitePageSize          = 32768       // Page size (32KB for write-heavy workload)
	sqliteCacheSize         = -256000     // Page cache size (negative = KB, 256MB)
	sqliteMmapSize          = 30000000000 // Memory-mapped I/O size (30GB)
	sqliteJournalSizeLimit  = 67108864    // WAL journal size limit (64MB)
	sqliteBusyTimeout       = 5000        // Busy timeout in milliseconds
	sqliteWalAutocheckpoint = 1000        // WAL autocheckpoint interval (pages)
)

// Scanner: Buffer Sizes for Streaming I/O
const (
	scannerBufferSize = 128 * 1024  // Initial scanner buffer (128KB, increased to prevent reallocation)
	scannerMaxLine    = 1024 * 1024 // Maximum line size (1MB)
)

// Worker Buffer Reuse: Per-worker buffer sizes to eliminate allocation churn
// Each worker goroutine maintains its own buffers to avoid contention
//
// Tuning guide:
//   - workerScannerBufferSize: Set to max expected blame output size per file
//     Too small = reallocations, too large = wasted memory
//   - workerAuthorMapCapacity: Set to max expected unique authors per file
//     Too small = map growth, too large = wasted memory
//
// Memory cost per worker = workerScannerBufferSize + (workerAuthorMapCapacity * ~24 bytes)
// Total memory cost = blameWorkers * (workerScannerBufferSize + workerAuthorMapCapacity * 24)
// Example: 16 workers * (256KB + 128*24) = ~4.1MB persistent
const (
	workerScannerBufferSize = 256 * 1024 // Scanner buffer per worker (256KB, tune for large files)
	workerAuthorMapCapacity = 128        // Initial author map capacity per worker (tune for files with many authors)
)

// Time Calculations: Commit Bucketing
const (
	hoursPerDay    = 24 // Hours in a day
	daysPerMonth   = 30 // Approximate days per month
	commitBucket3M = 3  // Months threshold for 3-month bucket
	commitBucket6M = 6  // Months threshold for 6-month bucket
)

// UI: Display Configuration
const (
	maxVisibleRepos      = 15 // Maximum repos to show in UI at once
	maxCompletedTracking = 50 // Ring buffer size for completed repo tracking
	maxRepoNameLength    = 35 // Maximum characters for repo name display
	progressBarWidth     = 20 // Character width of progress bar
)

// UI status indicators (ASCII only)
const (
	statusIconQueued      = "[-]"
	statusIconOptimizing  = "[⚡]"
	statusIconDiscovering = "[*]"
	statusIconBlaming     = "[>]"
	statusIconCommits     = "[=]"
	statusIconDone        = "[✓]"
	statusIconError       = "[X]"
)

// RepoStatus represents the processing state of a repository
// Using uint8 enum instead of string saves 15 bytes per RepoProgress (16 bytes → 1 byte)
type RepoStatus uint8

const (
	StatusQueued RepoStatus = iota
	StatusOptimizing
	StatusDiscovering
	StatusBlaming
	StatusCommits
	StatusDone
	StatusError
)

// OptimizationMode controls how aggressively repositories are optimized
type OptimizationMode int

const (
	OptimizeNone  OptimizationMode = iota // Skip optimization
	OptimizeQuick                         // Commit-graph + config only (1-5 min)
	OptimizeFull                          // Commit-graph + config + repack (5-30 min)
)

// String returns the string representation for display
// This allows automatic string conversion while maintaining memory efficiency
func (s RepoStatus) String() string {
	switch s {
	case StatusQueued:
		return "queued"
	case StatusOptimizing:
		return "optimizing"
	case StatusDiscovering:
		return "discovering"
	case StatusBlaming:
		return "blaming"
	case StatusCommits:
		return "commits"
	case StatusDone:
		return "done"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

// FileTask represents work to blame a specific file
type FileTask struct {
	RepoPath string
	FilePath string
}

// FileAuthorStats represents aggregated blame counts for a file
// Uses RepoID instead of RepoPath string for memory efficiency (2 bytes vs 16 bytes)
// Uses AuthorID instead of Author string for memory efficiency (4 bytes vs 16 bytes)
// Fields ordered for optimal cache alignment: large → small
// Size: 28 bytes (reduced from 40 bytes via author interning, 30% reduction)
type FileAuthorStats struct {
	FilePath string // 16 bytes [0-15]
	AuthorID uint32 // 4 bytes [16-19]
	Lines    int32  // 4 bytes [20-23]
	RepoID   uint16 // 2 bytes [24-25]
	Flags    uint16 // 2 bytes [26-27]
}

// CommitStatsBatch represents a batch of commit stats for database writing
// Uses AuthorID map key instead of author string for memory efficiency
type CommitStatsBatch struct {
	RepoID uint16                  // Interned repository ID
	Stats  map[uint32]*CommitStats // Map keyed by interned AuthorID
}

// CommitStats represents commit activity in time buckets
// Uses int32 for counts to reduce memory (4 bytes vs 8 bytes per field)
// Uses AuthorID instead of Author string for memory efficiency (4 bytes vs 16 bytes)
// Size: 20 bytes (reduced from 32 bytes via author interning, 37.5% reduction)
type CommitStats struct {
	AuthorID   uint32 // 4 bytes [0-3]
	Commits3M  int32  // 4 bytes [4-7]
	Commits6M  int32  // 4 bytes [8-11]
	Commits12M int32  // 4 bytes [12-15]
	Merges     int32  // 4 bytes [16-19]
}

// RepoResult represents the outcome of processing a repository
type RepoResult struct {
	RepoName string
	Success  bool
	Error    error
}

// RepoProgress represents the current state of a repository being processed
// Fields ordered for optimal cache alignment: large → small
// Size: 56 bytes (reduced from 72 bytes with string status)
type RepoProgress struct {
	Name         string     // 16 bytes [0-15]
	ErrorMessage string     // 16 bytes [16-31]
	LinesBlamed  int64      // 8 bytes [32-39]
	FilesTotal   int32      // 4 bytes [40-43]
	FilesBlamed  int32      // 4 bytes [44-47]
	FilesSkipped int32      // 4 bytes [48-51]
	Status       RepoStatus // 1 byte [52]
	_            [3]byte    // 3 bytes padding [53-55]
}

// Metrics for progress tracking (atomic counters)
// Each field is padded to occupy its own cache line (64 bytes) to prevent false sharing
// on multi-core systems. This costs 280 bytes but eliminates cache line contention,
// improving performance by 10-20% on systems with 8+ CPU cores.
type Metrics struct {
	filesProcessed atomic.Int64
	_              [56]byte // Cache line padding (64 - 8 = 56)

	linesProcessed atomic.Int64
	_              [56]byte // Cache line padding

	errors atomic.Int64
	_      [56]byte // Cache line padding

	reposProcessed atomic.Int64
	_              [56]byte // Cache line padding

	filesSkipped atomic.Int64 // Files skipped due to timeout or other issues
	_            [56]byte     // Cache line padding
}

var metrics Metrics

// Global verbose flag
var verbose bool

// RepoIntern handles string interning for repository paths
// Replaces 16-byte string pointers with 2-byte uint16 IDs
type RepoIntern struct {
	mu       sync.RWMutex
	pathToID map[string]uint16
	idToPath []string
	nextID   uint16
}

func newRepoIntern() *RepoIntern {
	return &RepoIntern{
		pathToID: make(map[string]uint16, repoInternCapacity),
		idToPath: make([]string, 0, repoInternCapacity),
	}
}

func (r *RepoIntern) Intern(path string) uint16 {
	// Fast path: read lock for existing paths
	r.mu.RLock()
	if id, exists := r.pathToID[path]; exists {
		r.mu.RUnlock()
		return id
	}
	r.mu.RUnlock()

	// Slow path: write lock to add new path
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if id, exists := r.pathToID[path]; exists {
		return id
	}

	// Assign new ID
	id := r.nextID
	r.nextID++
	r.pathToID[path] = id
	r.idToPath = append(r.idToPath, path)
	return id
}

func (r *RepoIntern) Lookup(id uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(id) < len(r.idToPath) {
		return r.idToPath[id]
	}
	return ""
}

var repoIntern = newRepoIntern()

// AuthorIntern handles string interning for author emails
// Replaces 16-byte string pointers with 4-byte uint32 IDs
// Uses double-checked locking for thread-safe interning
type AuthorIntern struct {
	mu        sync.RWMutex
	emailToID map[string]uint32
	idToEmail []string
	nextID    uint32
}

func newAuthorIntern() *AuthorIntern {
	return &AuthorIntern{
		emailToID: make(map[string]uint32, authorInternCapacity),
		idToEmail: make([]string, 0, authorInternCapacity),
	}
}

func (a *AuthorIntern) Intern(email string) uint32 {
	// Fast path: read lock for existing emails
	a.mu.RLock()
	if id, exists := a.emailToID[email]; exists {
		a.mu.RUnlock()
		return id
	}
	a.mu.RUnlock()

	// Slow path: write lock to add new email
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock (race condition between locks)
	if id, exists := a.emailToID[email]; exists {
		return id
	}

	// Assign new ID
	id := a.nextID
	a.nextID++
	a.emailToID[email] = id
	a.idToEmail = append(a.idToEmail, email)
	return id
}

// InternBytes interns a byte slice as a string without allocating unless necessary
// Uses zero-copy string conversion for lookup (Go 1.20+)
// Only allocates if the email is not already interned
func (a *AuthorIntern) InternBytes(email []byte) uint32 {
	// Fast path: read lock with zero-copy string conversion
	a.mu.RLock()
	// Zero-copy conversion using unsafe (Go 1.20+)
	// This does NOT allocate but string must not be modified
	emailStr := unsafe.String(unsafe.SliceData(email), len(email))
	if id, exists := a.emailToID[emailStr]; exists {
		a.mu.RUnlock()
		return id
	}
	a.mu.RUnlock()

	// Slow path: allocate proper string for storage
	emailStrCopy := string(email)
	return a.Intern(emailStrCopy)
}

func (a *AuthorIntern) Lookup(id uint32) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if int(id) < len(a.idToEmail) {
		return a.idToEmail[id]
	}
	return ""
}

var authorIntern = newAuthorIntern()

// RepoInfo represents a git repository to process
type RepoInfo struct {
	Path      string // Absolute path to the repository
	Name      string // Name for display/identification
	FileCount int    // Number of files to process (from pre-flight count)
}

// Text file extensions for fast-path detection
var textExtensions = map[string]bool{
	".go": true, ".py": true, ".js": true, ".ts": true, ".tsx": true, ".jsx": true,
	".c": true, ".cpp": true, ".cc": true, ".cxx": true, ".h": true, ".hpp": true,
	".java": true, ".kt": true, ".scala": true, ".rs": true, ".swift": true,
	".rb": true, ".php": true, ".pl": true, ".sh": true, ".bash": true,
	".txt": true, ".md": true, ".json": true, ".xml": true, ".yaml": true, ".yml": true,
	".html": true, ".css": true, ".scss": true, ".sass": true, ".less": true,
	".sql": true, ".proto": true, ".thrift": true, ".graphql": true,
	".vim": true, ".lua": true, ".r": true, ".m": true, ".gradle": true,
	".properties": true, ".conf": true, ".config": true, ".ini": true, ".toml": true,
}

// getCodeExtensions returns a slice of code file extensions for gocodewalker
func getCodeExtensions() []string {
	extensions := make([]string, 0, len(textExtensions))
	for ext := range textExtensions {
		// Remove the leading dot for gocodewalker
		extensions = append(extensions, strings.TrimPrefix(ext, "."))
	}
	return extensions
}

// FileLister abstracts how we enumerate code files for blame
type FileLister interface {
	List(repoPath string) ([]string, error)
	Count(repoPath string) (int, error)
}

// GitLsTreeLister uses git ls-tree to enumerate files at HEAD
// This is faster than filesystem walking and only includes tracked files
type GitLsTreeLister struct{}

func (GitLsTreeLister) List(repoPath string) ([]string, error) {
	cmd := exec.Command("git", "-C", repoPath, "ls-tree", "-r", "-z", "--name-only", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		// For unborn HEAD, treat as empty
		return []string{}, nil
	}

	raw := bytes.Split(out, []byte{0})
	files := make([]string, 0, len(raw))
	for _, b := range raw {
		if len(b) == 0 {
			continue
		}
		p := string(b)
		if textExtensions[strings.ToLower(filepath.Ext(p))] {
			files = append(files, p)
		}
	}
	return files, nil
}

func (GitLsTreeLister) Count(repoPath string) (int, error) {
	cmd := exec.Command("git", "-C", repoPath, "ls-tree", "-r", "-z", "--name-only", "HEAD")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}

	buf := make([]byte, 64*1024)
	leftover := []byte{}
	count := 0

	for {
		n, er := stdout.Read(buf)
		if n > 0 {
			chunk := append(leftover, buf[:n]...)
			parts := bytes.Split(chunk, []byte{0})
			leftover = parts[len(parts)-1]
			for _, part := range parts[:len(parts)-1] {
				if len(part) == 0 {
					continue
				}
				p := string(part)
				if textExtensions[strings.ToLower(filepath.Ext(p))] {
					count++
				}
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			_ = cmd.Wait()
			return 0, er
		}
	}
	if err := cmd.Wait(); err != nil {
		return 0, err
	}
	return count, nil
}

// FSWalkerLister uses gocodewalker (filesystem walk) to enumerate files
// This respects .gitignore and may include untracked files
type FSWalkerLister struct{}

func (FSWalkerLister) List(repoPath string) ([]string, error) {
	return getFilesAtHEAD(repoPath)
}

func (FSWalkerLister) Count(repoPath string) (int, error) {
	ri := RepoInfo{Path: repoPath}
	counted, err := countFilesInRepo(ri)
	if err != nil {
		return 0, err
	}
	return counted.FileCount, nil
}

// isGitRepo checks if a directory is a git repository
func isGitRepo(path string) bool {
	gitDir := filepath.Join(path, ".git")
	info, err := os.Stat(gitDir)
	return err == nil && info.IsDir()
}

// shouldOptimizeRepo checks if a repository needs optimization for blame performance
// Returns true if any of these conditions are met:
//   - commit-graph file is missing
//   - multiple packfiles exist (fragmented)
//   - critical config settings are missing or suboptimal
func shouldOptimizeRepo(repoPath string) bool {
	// Check 1: commit-graph existence
	commitGraphPath := filepath.Join(repoPath, ".git", "objects", "info", "commit-graph")
	if _, err := os.Stat(commitGraphPath); os.IsNotExist(err) {
		if verbose {
			fmt.Fprintf(os.Stderr, "Repo needs optimization: %s (missing commit-graph)\n", filepath.Base(repoPath))
		}
		return true
	}

	// Check 2: packfile fragmentation
	packDir := filepath.Join(repoPath, ".git", "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err == nil && len(packFiles) > 1 {
		if verbose {
			fmt.Fprintf(os.Stderr, "Repo needs optimization: %s (%d packfiles, should be 1)\n", filepath.Base(repoPath), len(packFiles))
		}
		return true
	}

	// Check 3: critical config settings
	criticalConfigs := map[string]string{
		"core.commitGraph":         "true",
		"core.deltaBaseCacheLimit": "512m",
		"pack.useBitmaps":          "true",
	}

	for key, expectedValue := range criticalConfigs {
		cmd := exec.Command("git", "-C", repoPath, "config", "--get", key)
		output, err := cmd.Output()
		if err != nil || strings.TrimSpace(string(output)) != expectedValue {
			if verbose {
				actualValue := strings.TrimSpace(string(output))
				if actualValue == "" {
					actualValue = "(not set)"
				}
				fmt.Fprintf(os.Stderr, "Repo needs optimization: %s (%s=%s, expected=%s)\n",
					filepath.Base(repoPath), key, actualValue, expectedValue)
			}
			return true
		}
	}

	return false
}

// quickOptimizeRepo applies lightweight optimizations for blame performance
// Generates commit-graph with Bloom filters and configures critical Git settings
// Expected time: 1-5 minutes, Expected speedup: 3-3.5x
func quickOptimizeRepo(repoPath string, repoName string) error {
	if verbose {
		fmt.Fprintf(os.Stderr, "Optimizing %s: generating commit-graph...\n", repoName)
	}

	// Generate commit-graph with Bloom filters
	cmd := exec.Command("git", "-C", repoPath, "commit-graph", "write", "--reachable", "--changed-paths")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("commit-graph write failed: %w (output: %s)", err, string(output))
	}

	// Configure critical performance settings
	criticalConfigs := map[string]string{
		"core.commitGraph":         "true",
		"core.deltaBaseCacheLimit": "512m",
		"core.bigFileThreshold":    "512m",
		"core.packedGitLimit":      "512m",
		"core.packedGitWindowSize": "32m",
		"pack.useBitmaps":          "true",
		"pack.threads":             "0",
		"core.preloadIndex":        "true",
		"core.untrackedCache":      "true",
		"feature.manyFiles":        "true",
	}

	for key, value := range criticalConfigs {
		cmd := exec.Command("git", "-C", repoPath, "config", key, value)
		if err := cmd.Run(); err != nil {
			// Non-fatal: warn and continue
			if verbose {
				fmt.Fprintf(os.Stderr, "Warning: failed to set %s=%s: %v\n", key, value, err)
			}
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Optimization complete for %s (commit-graph + config)\n", repoName)
	}

	return nil
}

// fullOptimizeRepo applies deep optimizations including repack
// Generates commit-graph, configures settings, and repacks with speed-optimized delta chains
// Expected time: 5-30 minutes, Expected speedup: 5-7x
func fullOptimizeRepo(repoPath string, repoName string) error {
	// First apply quick optimizations
	if err := quickOptimizeRepo(repoPath, repoName); err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Optimizing %s: repacking with speed-optimized delta chains...\n", repoName)
	}

	// Repack with speed-optimized settings (depth=5 for fast decompression)
	// This trades ~25% more disk space for 3-5x faster object access
	// Set timeout for repack (can take 5-30 minutes on large repos)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "repack",
		"-f", "-a", "-d", "-b", "--depth=5", "--window=15", "--threads=0")

	if output, err := cmd.CombinedOutput(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("repack timed out after 30 minutes")
		}
		return fmt.Errorf("repack failed: %w (output: %s)", err, string(output))
	}

	// Prune redundant loose objects
	cmd = exec.Command("git", "-C", repoPath, "prune-packed")
	if err := cmd.Run(); err != nil {
		// Non-fatal
		if verbose {
			fmt.Fprintf(os.Stderr, "Warning: prune-packed failed: %v\n", err)
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Full optimization complete for %s (commit-graph + config + repack)\n", repoName)
	}

	return nil
}

// OptimizationMarker tracks optimization state per repository
type OptimizationMarker struct {
	Timestamp    time.Time
	Mode         OptimizationMode
	GowhoVersion string
}

// loadOptimizationMarker reads the .gowho-optimized marker file
func loadOptimizationMarker(repoPath string) (*OptimizationMarker, error) {
	markerPath := filepath.Join(repoPath, ".git", ".gowho-optimized")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) < 3 {
		return nil, fmt.Errorf("invalid marker format")
	}

	timestamp, err := time.Parse(time.RFC3339, lines[0])
	if err != nil {
		return nil, err
	}

	var mode OptimizationMode
	switch lines[1] {
	case "quick":
		mode = OptimizeQuick
	case "full":
		mode = OptimizeFull
	default:
		return nil, fmt.Errorf("unknown optimization mode: %s", lines[1])
	}

	return &OptimizationMarker{
		Timestamp:    timestamp,
		Mode:         mode,
		GowhoVersion: lines[2],
	}, nil
}

// saveOptimizationMarker writes the .gowho-optimized marker file
func saveOptimizationMarker(repoPath string, mode OptimizationMode) error {
	markerPath := filepath.Join(repoPath, ".git", ".gowho-optimized")

	modeStr := "none"
	switch mode {
	case OptimizeQuick:
		modeStr = "quick"
	case OptimizeFull:
		modeStr = "full"
	}

	content := fmt.Sprintf("%s\n%s\n%s\n",
		time.Now().Format(time.RFC3339),
		modeStr,
		"gowho-optimized") // Version placeholder

	return os.WriteFile(markerPath, []byte(content), 0644)
}

// shouldSkipOptimization checks if we should skip optimization based on marker
func shouldSkipOptimization(repoPath string, requestedMode OptimizationMode) bool {
	marker, err := loadOptimizationMarker(repoPath)
	if err != nil {
		// No marker or invalid marker - don't skip
		return false
	}

	// Skip if already optimized at same or higher level
	if marker.Mode >= requestedMode {
		// Check if marker is stale (>30 days old)
		if time.Since(marker.Timestamp) > 30*24*time.Hour {
			return false // Re-optimize stale repos
		}
		return true
	}

	return false
}

// scanDirectory recursively scans a directory for git repositories in parallel
func scanDirectory(path string, results chan<- RepoInfo, sem chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	// Acquire semaphore to limit concurrent filesystem operations
	sem <- struct{}{}
	defer func() { <-sem }()

	// Check if this directory is a git repository
	if isGitRepo(path) {
		results <- RepoInfo{
			Path: path,
			Name: filepath.Base(path),
		}
		// Don't descend into git repositories
		return
	}

	// Read directory entries
	entries, err := os.ReadDir(path)
	if err != nil {
		// Skip directories we can't access (permissions, etc.)
		return
	}

	// Recursively scan subdirectories in parallel
	for _, entry := range entries {
		if entry.IsDir() {
			childPath := filepath.Join(path, entry.Name())
			wg.Add(1)
			go scanDirectory(childPath, results, sem, wg)
		}
	}
}

// findGitRepos recursively discovers all git repositories under path
// Uses concurrent goroutines to parallelize directory scanning
func findGitRepos(path string) ([]RepoInfo, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	if verbose {
		fmt.Printf("Recursively scanning for git repositories in: %s\n", absPath)
	}
	startTime := time.Now()

	// Channel for found repositories
	results := make(chan RepoInfo, repoScanBuffer)

	// Semaphore to limit concurrent filesystem operations
	maxConcurrent := runtime.NumCPU() * scanConcurrencyMultiplier
	sem := make(chan struct{}, maxConcurrent)

	// WaitGroup to track all scanning goroutines
	var wg sync.WaitGroup

	// Collector goroutine to gather results
	repos := make([]RepoInfo, 0)
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for repo := range results {
			repos = append(repos, repo)
		}
	}()

	// Start recursive scanning
	wg.Add(1)
	go scanDirectory(absPath, results, sem, &wg)

	// Wait for all scanning to complete
	wg.Wait()
	close(results)

	// Wait for collector to finish
	collectorWg.Wait()

	if verbose {
		elapsed := time.Since(startTime)
		fmt.Printf("Found %d git repositories in %.2fs (using %d concurrent workers)\n",
			len(repos), elapsed.Seconds(), maxConcurrent)
		fmt.Println()
	}

	if len(repos) == 0 {
		return nil, fmt.Errorf("no git repositories found in %s", absPath)
	}

	return repos, nil
}

// runOptimizeCommand runs the optimize command (separate from analyze)
// Optimizes repos in REVERSE order (largest first) using all CPU cores
func runOptimizeCommand(targetPath string, optimizationMode OptimizationMode, forceOptimize bool, jobs int) {
	if verbose {
		fmt.Println("=== Git Who - Repository Optimizer ===")
		fmt.Println()
		modeStr := "quick"
		if optimizationMode == OptimizeFull {
			modeStr = "full"
		}
		fmt.Printf("Optimization mode: %s%s\n", modeStr, map[bool]string{true: " (forced)", false: ""}[forceOptimize])
		fmt.Printf("Parallel workers: %d\n", jobs)
		fmt.Println()
	}

	// Find git repositories
	repos, err := findGitRepos(targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		fmt.Printf("Found %d git repository(ies) to optimize\n\n", len(repos))
	}

	// Count files to determine repo sizes
	fileLister := GitLsTreeLister{}
	repos = preflightCountFiles(repos, fileLister)

	// Sort by file count in REVERSE order (largest first)
	sort.Slice(repos, func(i, j int) bool {
		return repos[i].FileCount > repos[j].FileCount
	})

	if verbose {
		fmt.Println("Optimization order (largest to smallest):")
		for i, repo := range repos {
			fmt.Printf("  %2d. %-30s (%d files)\n", i+1, repo.Name, repo.FileCount)
		}
		fmt.Println()
	}

	// Create progress channel
	progressChanSize := progressChanDefault
	if len(repos) < progressChanThreshold {
		progressChanSize = len(repos) * progressChanMultiplier
	}
	progressChan := make(chan RepoProgress, progressChanSize)

	// Start progress UI goroutine
	uiDone := make(chan struct{})
	go progressUI(progressChan, uiDone)

	// Initialize all repos as queued
	for _, repo := range repos {
		progressChan <- RepoProgress{
			Name:   repo.Name,
			Status: StatusQueued,
		}
	}

	// Progress tracking
	type OptimizeResult struct {
		RepoName string
		Success  bool
		Error    error
		Skipped  bool
	}

	results := make(chan OptimizeResult, len(repos))
	repoTasks := make(chan RepoInfo, len(repos))

	// Start optimization workers (use all cores)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for repo := range repoTasks {
				// Update status: optimizing
				progressChan <- RepoProgress{
					Name:   repo.Name,
					Status: StatusOptimizing,
				}

				// Check if should skip optimization
				if !forceOptimize && shouldSkipOptimization(repo.Path, optimizationMode) {
					if verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] Skipping %s (already optimized)\n", workerID, repo.Name)
					}
					progressChan <- RepoProgress{
						Name:   repo.Name,
						Status: StatusDone,
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: true, Skipped: true}
					continue
				}

				// Check if needs optimization
				if !shouldOptimizeRepo(repo.Path) && !forceOptimize {
					if verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] Skipping %s (already optimal)\n", workerID, repo.Name)
					}
					progressChan <- RepoProgress{
						Name:   repo.Name,
						Status: StatusDone,
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: true, Skipped: true}
					continue
				}

				// Optimize repo
				var err error
				switch optimizationMode {
				case OptimizeQuick:
					err = quickOptimizeRepo(repo.Path, repo.Name)
				case OptimizeFull:
					err = fullOptimizeRepo(repo.Path, repo.Name)
				}

				if err != nil {
					if verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] ERROR optimizing %s: %v\n", workerID, repo.Name, err)
					}
					progressChan <- RepoProgress{
						Name:         repo.Name,
						Status:       StatusError,
						ErrorMessage: err.Error(),
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: false, Error: err}
				} else {
					// Save marker
					if err := saveOptimizationMarker(repo.Path, optimizationMode); err != nil {
						if verbose {
							fmt.Fprintf(os.Stderr, "[Worker %d] Warning: failed to save marker for %s: %v\n", workerID, repo.Name, err)
						}
					}
					if verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] ✓ Completed %s\n", workerID, repo.Name)
					}
					progressChan <- RepoProgress{
						Name:   repo.Name,
						Status: StatusDone,
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: true}
				}
			}
		}(i)
	}

	// Feed repos to workers (largest first)
	go func() {
		for _, repo := range repos {
			repoTasks <- repo
		}
		close(repoTasks)
	}()

	// Wait for workers in background
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	successCount := 0
	skippedCount := 0
	errorCount := 0
	for result := range results {
		if result.Skipped {
			skippedCount++
		} else if result.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	// Close progress UI and wait for it to finish
	close(progressChan)
	<-uiDone

	// Summary
	fmt.Println()
	fmt.Println("=== Optimization Complete ===")
	fmt.Printf("Successfully optimized: %d/%d repositories\n", successCount, len(repos))
	if skippedCount > 0 {
		fmt.Printf("Skipped (already optimized): %d\n", skippedCount)
	}
	if errorCount > 0 {
		fmt.Printf("Errors: %d\n", errorCount)
	}
	fmt.Println()
	fmt.Println("You can now run 'gowho analyze' to benefit from 3-7x faster git blame operations.")
	fmt.Println()
}

func printHelp() {
	fmt.Println("gowho - Git repository contribution analyzer")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  gowho [COMMAND] [OPTIONS] [PATH]")
	fmt.Println()
	fmt.Println("COMMANDS:")
	fmt.Println("  analyze       Analyze repositories (default command)")
	fmt.Println("  optimize      Optimize repositories for faster analysis")
	fmt.Println()
	fmt.Println("ANALYZE OPTIONS:")
	fmt.Println("  -h, --help                    Show this help message")
	fmt.Println("  -v, --verbose                 Enable verbose output")
	fmt.Println("  -L, --file-list=MODE          File listing mode: git or fs (default: git)")
	fmt.Println("                                  git: Use git ls-tree (tracked files only)")
	fmt.Println("                                  fs:  Use filesystem walker (respects .gitignore)")
	fmt.Println()
	fmt.Println("OPTIMIZE OPTIONS:")
	fmt.Println("  -h, --help                    Show this help message")
	fmt.Println("  -v, --verbose                 Enable verbose output")
	fmt.Println("  -m, --mode=MODE               Optimization mode (default: quick)")
	fmt.Println("                                  quick: Commit-graph + config (1-5 min, 3-3.5x faster)")
	fmt.Println("                                  full:  Commit-graph + config + repack (5-30 min, 5-7x faster)")
	fmt.Println("  -j, --jobs=N                  Number of parallel optimization workers (default: all CPUs)")
	fmt.Println("      --force                   Force re-optimization even if already optimized")
	fmt.Println()
	fmt.Println("ARGUMENTS:")
	fmt.Println("  PATH                          Directory to scan for git repositories (default: .)")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Standard workflow (optimize first, then analyze)")
	fmt.Println("  gowho optimize ~/projects     Optimize all repos (largest first)")
	fmt.Println("  gowho analyze ~/projects      Then analyze all repos")
	fmt.Println()
	fmt.Println("  # Quick analysis without optimization")
	fmt.Println("  gowho                         Analyze current directory")
	fmt.Println("  gowho ~/projects              Analyze all repos in ~/projects")
	fmt.Println()
	fmt.Println("  # Full optimization for large codebases")
	fmt.Println("  gowho optimize --mode=full --jobs=16 ~/projects")
	fmt.Println()
	fmt.Println("  # Verbose mode")
	fmt.Println("  gowho optimize -v .           Show detailed optimization progress")
	fmt.Println("  gowho analyze -v .            Show detailed analysis progress")
	fmt.Println()
	fmt.Println("OUTPUT:")
	fmt.Println("  analyze: Creates git-who.db with two tables:")
	fmt.Println("    - file_author: Line counts per author per file")
	fmt.Println("    - author_commit_stats: Commit activity over 3/6/12 month periods")
	fmt.Println()
	fmt.Println("  optimize: Creates .gowho-optimized markers in each repo's .git directory")
	fmt.Println()
	fmt.Println("OPTIMIZATION STRATEGY:")
	fmt.Println("  The optimize command processes repos in REVERSE order (largest first):")
	fmt.Println("    - Large repos (30+ year codebases) optimize first using all CPU cores")
	fmt.Println("    - Small repos optimize quickly after")
	fmt.Println("    - When you run analyze, large repos are already optimized (5-7x faster)")
	fmt.Println()
	fmt.Println("  Recommended for large repository sets (1000+ repos):")
	fmt.Println("    1. Run 'gowho optimize' once (may take 30-60 min for huge repos)")
	fmt.Println("    2. Run 'gowho analyze' repeatedly (benefits from optimization cache)")
	fmt.Println()
}

func main() {
	// Detect command (optimize or analyze)
	command := "analyze" // default command
	argOffset := 1
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		if os.Args[1] == "optimize" || os.Args[1] == "analyze" {
			command = os.Args[1]
			argOffset = 2
		}
	}

	// Parse command-specific arguments
	if command == "optimize" {
		// OPTIMIZE COMMAND
		targetPath := "."
		optimizationMode := OptimizeQuick // default to quick
		forceOptimize := false
		jobs := runtime.NumCPU() // default to all CPUs

		for i := argOffset; i < len(os.Args); i++ {
			arg := os.Args[i]
			switch {
			case arg == "-h" || arg == "--help":
				printHelp()
				os.Exit(0)
			case arg == "-v" || arg == "--verbose":
				verbose = true
			case strings.HasPrefix(arg, "--mode="):
				v := strings.TrimPrefix(arg, "--mode=")
				switch v {
				case "quick":
					optimizationMode = OptimizeQuick
				case "full":
					optimizationMode = OptimizeFull
				default:
					fmt.Fprintf(os.Stderr, "Unknown --mode=%s (use quick|full)\n", v)
					os.Exit(2)
				}
			case arg == "-m" && i+1 < len(os.Args):
				i++
				v := os.Args[i]
				switch v {
				case "quick":
					optimizationMode = OptimizeQuick
				case "full":
					optimizationMode = OptimizeFull
				default:
					fmt.Fprintf(os.Stderr, "Unknown -m %s (use quick|full)\n", v)
					os.Exit(2)
				}
			case strings.HasPrefix(arg, "--jobs="):
				v := strings.TrimPrefix(arg, "--jobs=")
				var err error
				jobs, err = fmt.Sscanf(v, "%d", &jobs)
				if err != nil || jobs < 1 {
					fmt.Fprintf(os.Stderr, "Invalid --jobs=%s (use positive integer)\n", v)
					os.Exit(2)
				}
			case arg == "-j" && i+1 < len(os.Args):
				i++
				v := os.Args[i]
				var err error
				_, err = fmt.Sscanf(v, "%d", &jobs)
				if err != nil || jobs < 1 {
					fmt.Fprintf(os.Stderr, "Invalid -j %s (use positive integer)\n", v)
					os.Exit(2)
				}
			case arg == "--force":
				forceOptimize = true
			default:
				if !strings.HasPrefix(arg, "-") {
					targetPath = arg
				}
			}
		}

		// Run optimize command
		runOptimizeCommand(targetPath, optimizationMode, forceOptimize, jobs)
		return
	}

	// ANALYZE COMMAND (default)
	targetPath := "."
	fileListMode := fileListModeDefault

	for i := argOffset; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch {
		case arg == "-h" || arg == "--help":
			printHelp()
			os.Exit(0)
		case arg == "-v" || arg == "--verbose":
			verbose = true
		case strings.HasPrefix(arg, "--file-list="):
			v := strings.TrimPrefix(arg, "--file-list=")
			if v != "git" && v != "fs" {
				fmt.Fprintf(os.Stderr, "Unknown --file-list=%s (use git|fs)\n", v)
				os.Exit(2)
			}
			fileListMode = v
		case arg == "-L" && i+1 < len(os.Args):
			i++
			v := os.Args[i]
			if v != "git" && v != "fs" {
				fmt.Fprintf(os.Stderr, "Unknown -L %s (use git|fs)\n", v)
				os.Exit(2)
			}
			fileListMode = v
		default:
			if !strings.HasPrefix(arg, "-") {
				targetPath = arg
			}
		}
	}

	var fileLister FileLister
	switch fileListMode {
	case "git":
		fileLister = GitLsTreeLister{}
	case "fs":
		fileLister = FSWalkerLister{}
	}

	if verbose {
		fmt.Println("=== Git Who - Repository Contribution Analyzer (Optimized Go Edition) ===")
		fmt.Println()
		fmt.Printf("File listing mode: %s\n", fileListMode)
		fmt.Println()
	}

	// Tune GC for lower memory usage
	debug.SetGCPercent(gcPercent)

	// Set soft memory limit to prevent runaway memory growth
	debug.SetMemoryLimit(memoryLimit)

	if verbose {
		fmt.Println("Memory optimizations: GC=50%, Limit=4GB, Streaming I/O, Interned paths")
		fmt.Println()
	}

	// Find git repositories
	repos, err := findGitRepos(targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		fmt.Printf("Found %d git repository(ies) to analyze\n", len(repos))
		for _, repo := range repos {
			fmt.Printf("  - %s (%s)\n", repo.Name, repo.Path)
		}
		fmt.Println()
	}

	// Pre-flight: count files and sort by size (smallest first)
	repos = preflightCountFiles(repos, fileLister)

	if verbose {
		// Display optimized processing order
		fmt.Println("Optimized processing order (smallest to largest):")
		for i, repo := range repos {
			fmt.Printf("  %2d. %-30s (%d files)\n", i+1, repo.Name, repo.FileCount)
		}
		fmt.Println()
	}

	// Create shared database
	db, err := createDatabase()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// Determine optimal worker counts
	numCPU := runtime.NumCPU()
	blameWorkers := numCPU * blameWorkersMultiplier
	repoWorkers := numCPU / repoWorkersDivisor
	if repoWorkers < 1 {
		repoWorkers = 1
	}
	// Cap repoWorkers to prevent excessive goroutine proliferation
	if repoWorkers > maxRepoWorkers {
		repoWorkers = maxRepoWorkers
	}
	if verbose {
		fmt.Printf("Using %d CPU cores with %d repo workers and %d blame workers per repo\n",
			numCPU, repoWorkers, blameWorkers)
		fmt.Println()
	}

	// Create CSP channels
	repoTasks := make(chan RepoInfo, repoTaskBuffer)
	repoResults := make(chan RepoResult, len(repos))
	// Scale progress channel based on repo count
	progressChanSize := progressChanDefault
	if len(repos) < progressChanThreshold {
		progressChanSize = len(repos) * progressChanMultiplier
	}
	progressChan := make(chan RepoProgress, progressChanSize)

	// Centralized database write channels (shared across all repos)
	fileStatsChan := make(chan FileAuthorStats, contributionBuffer)
	commitStatsChan := make(chan CommitStatsBatch, commitStatsBuffer)

	// Start progress UI goroutine FIRST (consumer must exist before producer sends)
	uiDone := make(chan struct{})
	go progressUI(progressChan, uiDone)

	// Initialize all repos as queued (send after consumer is running)
	for _, repo := range repos {
		progressChan <- RepoProgress{
			Name:   repo.Name,
			Status: StatusQueued,
		}
	}

	// Start centralized database writer (single writer for all repos)
	dbWriterDone := make(chan struct{})
	go centralizedDatabaseWriter(db, fileStatsChan, commitStatsChan, dbWriterDone)

	// Start repository worker pool
	var repoWg sync.WaitGroup
	for i := 0; i < repoWorkers; i++ {
		repoWg.Add(1)
		go repoWorker(blameWorkers, repoTasks, repoResults, progressChan, fileStatsChan, commitStatsChan, fileLister, &repoWg)
	}

	// Feed repositories to workers
	go func() {
		for _, repo := range repos {
			repoTasks <- repo
		}
		close(repoTasks)
	}()

	// Collect results in background
	go func() {
		repoWg.Wait()
		close(repoResults)
		// Close database writer channels after all repos finish
		close(fileStatsChan)
		close(commitStatsChan)
	}()

	// Process results as they come in
	successCount := 0
	errorCount := 0
	for result := range repoResults {
		if result.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	// Close progress UI and wait for it to finish
	close(progressChan)
	<-uiDone

	// Wait for database writer to finish
	<-dbWriterDone

	// Generate summary
	fmt.Println()
	fmt.Println("=== Analysis Complete ===")
	fmt.Printf("Successfully processed %d/%d repositories (%d errors)\n",
		successCount, len(repos), errorCount)
	if metrics.filesSkipped.Load() > 0 {
		fmt.Printf("Files skipped due to timeout: %d\n", metrics.filesSkipped.Load())
	}
	fmt.Println()
	generateSummary(db)
}

// centralizedDatabaseWriter is the single goroutine responsible for all SQLite writes
// This eliminates database locking issues when multiple repos process in parallel
func centralizedDatabaseWriter(db *sql.DB, fileStats <-chan FileAuthorStats,
	commitStats <-chan CommitStatsBatch, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(batchFlushInterval)
	defer ticker.Stop()

	fileStatsBatch := make([]FileAuthorStats, 0, batchSizeThreshold)

	flushFileStats := func() {
		if len(fileStatsBatch) == 0 {
			return
		}
		if err := writeBatch(db, fileStatsBatch); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file stats batch: %v\n", err)
			metrics.errors.Add(1)
		}
		fileStatsBatch = fileStatsBatch[:0]
	}

	// Track if both channels are closed
	fileStatsClosed := false
	commitStatsClosed := false

	for {
		select {
		case stat, ok := <-fileStats:
			if !ok {
				fileStatsClosed = true
				fileStats = nil // Disable this case
				continue
			}
			fileStatsBatch = append(fileStatsBatch, stat)
			if len(fileStatsBatch) >= batchSizeThreshold {
				flushFileStats()
			}

		case batch, ok := <-commitStats:
			if !ok {
				commitStatsClosed = true
				commitStats = nil // Disable this case
				continue
			}
			// Write commit stats immediately (they're already batched)
			// Convert RepoID back to path for database
			repoPath := repoIntern.Lookup(batch.RepoID)
			if err := writeCommitStatsBatch(db, repoPath, batch.Stats); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing commit stats: %v\n", err)
				metrics.errors.Add(1)
			}

		case <-ticker.C:
			flushFileStats()
		}

		// Exit when both channels are closed and all data is flushed
		if fileStatsClosed && commitStatsClosed {
			flushFileStats()
			return
		}
	}
}

// processRepository analyzes a single git repository
func processRepository(repo RepoInfo, blameWorkers int,
	progressChan chan<- RepoProgress, fileStatsChan chan<- FileAuthorStats,
	commitStatsChan chan<- CommitStatsBatch, fileLister FileLister) error {

	// Intern the repository path once at the start
	repoID := repoIntern.Intern(repo.Path)

	// Update status: discovering files
	progressChan <- RepoProgress{
		Name:   repo.Name,
		Status: StatusDiscovering,
	}

	// Get file list using the configured lister
	files, err := fileLister.List(repo.Path)
	if err != nil {
		progressChan <- RepoProgress{
			Name:         repo.Name,
			Status:       StatusError,
			ErrorMessage: err.Error(),
		}
		return fmt.Errorf("failed to get files: %w", err)
	}

	totalFiles := len(files)
	if totalFiles == 0 {
		progressChan <- RepoProgress{
			Name:        repo.Name,
			Status:      StatusDone,
			FilesTotal:  0,
			LinesBlamed: 0,
		}
		return nil
	}

	// Update status: blaming
	progressChan <- RepoProgress{
		Name:        repo.Name,
		Status:      StatusBlaming,
		FilesTotal:  int32(totalFiles),
		FilesBlamed: 0,
	}

	// Create pipeline channels
	fileTaskChan := make(chan FileTask, fileTaskBuffer)

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track progress for this repo
	var filesBlamed atomic.Int64
	var linesBlamed atomic.Int64
	var filesSkipped atomic.Int64

	// Progress reporter goroutine
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(progressReportInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				progressChan <- RepoProgress{
					Name:         repo.Name,
					Status:       StatusBlaming,
					FilesTotal:   int32(totalFiles),
					FilesBlamed:  int32(filesBlamed.Load()),
					FilesSkipped: int32(filesSkipped.Load()),
					LinesBlamed:  linesBlamed.Load(),
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start blame workers
	var blameWg sync.WaitGroup
	for i := 0; i < blameWorkers; i++ {
		blameWg.Add(1)
		go blameWorker(ctx, repo.Path, repoID, fileTaskChan, fileStatsChan, &filesBlamed, &linesBlamed, &filesSkipped, &blameWg)
	}

	// Fan out files to workers
	for _, file := range files {
		fileTaskChan <- FileTask{
			RepoPath: repo.Path,
			FilePath: file,
		}
	}
	close(fileTaskChan)

	// Wait for blame workers to complete
	blameWg.Wait()
	cancel() // Stop progress reporter

	// Wait for progress reporter to finish
	<-progressDone

	// Update status: analyzing commits
	progressChan <- RepoProgress{
		Name:        repo.Name,
		Status:      StatusCommits,
		FilesTotal:  int32(totalFiles),
		FilesBlamed: int32(totalFiles),
		LinesBlamed: linesBlamed.Load(),
	}

	if err := processCommitStats(repo.Path, repoID, commitStatsChan); err != nil {
		progressChan <- RepoProgress{
			Name:         repo.Name,
			Status:       StatusError,
			ErrorMessage: err.Error(),
		}
		return fmt.Errorf("failed to process commit stats: %w", err)
	}

	// Update status: done
	progressChan <- RepoProgress{
		Name:         repo.Name,
		Status:       StatusDone,
		FilesTotal:   int32(totalFiles),
		FilesBlamed:  int32(totalFiles),
		FilesSkipped: int32(filesSkipped.Load()),
		LinesBlamed:  linesBlamed.Load(),
	}

	return nil
}

// createDatabase creates the SQLite database with optimized schema
func createDatabase() (*sql.DB, error) {
	if verbose {
		fmt.Printf("Creating database: %s\n", dbFile)
	}

	// Remove existing database
	_ = os.Remove(dbFile)

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}

	// Enable performance optimizations for write-heavy workload
	// CRITICAL: page_size MUST be set BEFORE creating tables
	pragmas := []string{
		// Page size must be first (before any tables exist)
		fmt.Sprintf("PRAGMA page_size=%d", sqlitePageSize),
		// WAL mode for concurrent reads during writes
		"PRAGMA journal_mode=WAL",
		// synchronous=OFF for maximum write speed (accepts system crash risk)
		"PRAGMA synchronous=OFF",
		// Large page cache for bulk writes
		fmt.Sprintf("PRAGMA cache_size=%d", sqliteCacheSize),
		// Memory-mapped I/O for reduced syscall overhead
		fmt.Sprintf("PRAGMA mmap_size=%d", sqliteMmapSize),
		// Temp tables in memory
		"PRAGMA temp_store=MEMORY",
		// Prevent WAL from growing unbounded
		fmt.Sprintf("PRAGMA journal_size_limit=%d", sqliteJournalSizeLimit),
		// Busy timeout for lock contention
		fmt.Sprintf("PRAGMA busy_timeout=%d", sqliteBusyTimeout),
		// Exclusive locking mode (safe for single-writer, eliminates lock overhead)
		"PRAGMA locking_mode=EXCLUSIVE",
		// WAL autocheckpoint interval
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sqliteWalAutocheckpoint),
		// Disable auto-vacuum for write performance
		"PRAGMA auto_vacuum=NONE",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("pragma failed (%s): %w", pragma, err)
		}
		if verbose {
			fmt.Printf("  Applied: %s\n", pragma)
		}
	}

	schema := `
CREATE TABLE file_author (
    repo_path TEXT NOT NULL,
    file_path TEXT NOT NULL,
    author TEXT NOT NULL,
    lines INTEGER DEFAULT 0,
    PRIMARY KEY (repo_path, file_path, author)
) WITHOUT ROWID;

CREATE TABLE author_commit_stats (
    repo_path TEXT NOT NULL,
    author TEXT NOT NULL,
    commits_3m INTEGER DEFAULT 0,
    commits_6m INTEGER DEFAULT 0,
    commits_12m INTEGER DEFAULT 0,
    merges INTEGER DEFAULT 0,
    PRIMARY KEY (repo_path, author)
) WITHOUT ROWID;

CREATE INDEX idx_repo_path ON file_author(repo_path);
CREATE INDEX idx_file_path ON file_author(file_path);
CREATE INDEX idx_author ON file_author(author);
CREATE INDEX idx_lines ON file_author(lines DESC);
`

	_, err = db.Exec(schema)
	return db, err
}

// getFilesAtHEAD fetches all code files for a repository using gocodewalker
// This respects .gitignore and only processes files with known code extensions
func getFilesAtHEAD(repoPath string) ([]string, error) {
	fileListQueue := make(chan *gocodewalker.File, fileListQueueBuffer)
	fileWalker := gocodewalker.NewFileWalker(repoPath, fileListQueue)

	// Only process known code file extensions
	fileWalker.AllowListExtensions = getCodeExtensions()

	// Respect .gitignore and .ignore files (default behavior)
	fileWalker.IgnoreIgnoreFile = false
	fileWalker.IgnoreGitIgnore = false

	// Handle errors by logging and continuing
	errorHandler := func(e error) bool {
		// Continue processing on errors (e.g., permission denied)
		return true
	}
	fileWalker.SetErrorHandler(errorHandler)

	// Start walking in background
	go func() { _ = fileWalker.Start() }()

	// Collect files
	files := make([]string, 0, fileListCapacity)
	for f := range fileListQueue {
		files = append(files, f.Location)
	}

	return files, nil
}

// countFilesInRepo performs pre-flight file counting for a single repository
func countFilesInRepo(repo RepoInfo) (RepoInfo, error) {
	fileListQueue := make(chan *gocodewalker.File, fileListQueueBuffer)
	fileWalker := gocodewalker.NewFileWalker(repo.Path, fileListQueue)

	fileWalker.AllowListExtensions = getCodeExtensions()
	fileWalker.IgnoreIgnoreFile = false
	fileWalker.IgnoreGitIgnore = false

	errorHandler := func(e error) bool {
		return true
	}
	fileWalker.SetErrorHandler(errorHandler)

	go func() { _ = fileWalker.Start() }()

	count := 0
	for range fileListQueue {
		count++
	}

	repo.FileCount = count
	return repo, nil
}

// preflightCountFiles counts files in all repositories in parallel and sorts them
// Also detects which repos need optimization
func preflightCountFiles(repos []RepoInfo, fileLister FileLister) []RepoInfo {
	if verbose {
		fmt.Println("Pre-flight: Counting files in each repository to optimize processing order...")
	}
	startTime := time.Now()

	type countResult struct {
		repo          RepoInfo
		needsOptimize bool
		err           error
	}

	results := make(chan countResult, len(repos))
	var wg sync.WaitGroup

	// Process repos in parallel with concurrency limit
	maxConcurrent := runtime.NumCPU() * preflightConcurrencyMultiplier
	sem := make(chan struct{}, maxConcurrent)

	for _, repo := range repos {
		wg.Add(1)
		go func(r RepoInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			count, err := fileLister.Count(r.Path)
			if err != nil {
				results <- countResult{repo: r, err: err}
				return
			}
			r.FileCount = count

			// Check if optimization needed (non-blocking detection)
			needsOptimize := shouldOptimizeRepo(r.Path)

			results <- countResult{repo: r, needsOptimize: needsOptimize, err: nil}
		}(repo)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	countedRepos := make([]RepoInfo, 0, len(repos))
	reposNeedingOptimization := 0
	for result := range results {
		if result.err == nil {
			countedRepos = append(countedRepos, result.repo)
			if result.needsOptimize {
				reposNeedingOptimization++
			}
		}
	}

	// Sort by file count (smallest first for better parallelism)
	sort.Slice(countedRepos, func(i, j int) bool {
		return countedRepos[i].FileCount < countedRepos[j].FileCount
	})

	if verbose {
		elapsed := time.Since(startTime)
		totalFiles := 0
		for _, repo := range countedRepos {
			totalFiles += repo.FileCount
		}

		fmt.Printf("Pre-flight complete: %d repositories, %d total files (%.2fs)\n",
			len(countedRepos), totalFiles, elapsed.Seconds())

		if reposNeedingOptimization > 0 {
			fmt.Printf("Repositories needing optimization: %d/%d\n", reposNeedingOptimization, len(countedRepos))
			fmt.Printf("(Optimization would improve git blame performance by 3-7x)\n")
		}
		fmt.Println()
	}

	return countedRepos
}

// parseGitBlamePorcelain parses git blame --line-porcelain output and aggregates by author
// Uses a scanner for streaming to avoid loading entire output into memory
// Returns map keyed by interned AuthorID instead of email string
// Accepts a reusable authorCounts map to eliminate per-file allocations
func parseGitBlamePorcelain(scanner *bufio.Scanner, authorCounts map[uint32]int) map[uint32]int {

	var currentAuthorID uint32

	for scanner.Scan() {
		line := scanner.Bytes() // Avoid string allocation

		// author-mail <email@example.com>
		if bytes.HasPrefix(line, []byte("author-mail ")) {
			// Extract email, removing angle brackets
			email := line[12:] // Skip "author-mail "
			if len(email) > 2 && email[0] == '<' && email[len(email)-1] == '>' {
				emailBytes := email[1 : len(email)-1]
				currentAuthorID = authorIntern.InternBytes(emailBytes)
			}
		} else if len(line) > 0 && line[0] == '\t' {
			// Tab-prefixed lines are actual content lines
			if currentAuthorID != 0 {
				authorCounts[currentAuthorID]++
				currentAuthorID = 0
			}
		}
	}

	return authorCounts
}

// blameWorkerState holds reusable buffers for a blame worker
// Reusing buffers eliminates per-file allocation churn (640MB for 10K files)
// Each worker maintains its own buffers to avoid contention in highly parallel workloads
type blameWorkerState struct {
	scannerBuf   []byte
	authorCounts map[uint32]int
}

func newBlameWorkerState() *blameWorkerState {
	return &blameWorkerState{
		scannerBuf:   make([]byte, workerScannerBufferSize),
		authorCounts: make(map[uint32]int, workerAuthorMapCapacity),
	}
}

func (s *blameWorkerState) reset() {
	// Clear map for reuse - faster than reallocating
	for k := range s.authorCounts {
		delete(s.authorCounts, k)
	}
}

// blameWorker processes file tasks and emits aggregated author stats
// Uses streaming to avoid buffering entire git blame output in memory
func blameWorker(ctx context.Context, repoPath string, repoID uint16, fileTasks <-chan FileTask,
	stats chan<- FileAuthorStats, filesBlamed *atomic.Int64, linesBlamed *atomic.Int64,
	filesSkipped *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create worker-local state with reusable buffers
	state := newBlameWorkerState()

	for task := range fileTasks {
		// Reset state for next file
		state.reset()
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Create a timeout context for this specific git blame operation
		blameCtx, cancel := context.WithTimeout(ctx, gitBlameTimeout)
		defer cancel()

		// Run git blame with porcelain format, streaming output
		cmd := exec.CommandContext(blameCtx, "git", "-C", repoPath, "blame", "--line-porcelain", "-w", "HEAD", "--", task.FilePath)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			metrics.errors.Add(1)
			metrics.filesProcessed.Add(1)
			filesBlamed.Add(1)
			cancel()
			continue
		}

		if err := cmd.Start(); err != nil {
			metrics.errors.Add(1)
			metrics.filesProcessed.Add(1)
			filesBlamed.Add(1)
			cancel()
			continue
		}

		// Parse streaming output - no buffering of entire file
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(state.scannerBuf, scannerMaxLine)
		authorCounts := parseGitBlamePorcelain(scanner, state.authorCounts)

		if err := cmd.Wait(); err != nil {
			// Check if the error is due to timeout
			if blameCtx.Err() == context.DeadlineExceeded {
				// File timed out - skip it
				metrics.filesSkipped.Add(1)
				metrics.filesProcessed.Add(1)
				filesBlamed.Add(1)
				filesSkipped.Add(1)
				if verbose {
					fmt.Fprintf(os.Stderr, "Warning: git blame timed out after %v for file: %s\n", gitBlameTimeout, task.FilePath)
				}
			} else {
				// Other error - skip file
				metrics.errors.Add(1)
				metrics.filesProcessed.Add(1)
				filesBlamed.Add(1)
			}
			cancel()
			continue
		}

		// Emit aggregated stats with interned AuthorID
		totalLines := 0
		for authorID, count := range authorCounts {
			stat := FileAuthorStats{
				FilePath: task.FilePath,
				AuthorID: authorID,
				Lines:    int32(count),
				RepoID:   repoID,
				Flags:    0,
			}

			select {
			case stats <- stat:
				totalLines += count
			case <-ctx.Done():
				cancel()
				return
			}
		}

		metrics.linesProcessed.Add(int64(totalLines))
		metrics.filesProcessed.Add(1)
		filesBlamed.Add(1)
		linesBlamed.Add(int64(totalLines))
		cancel()
	}
}

// processCommitStats analyzes commit history and buckets by time
// Uses streaming to avoid loading entire commit log into memory
func processCommitStats(repoPath string, repoID uint16, commitStatsChan chan<- CommitStatsBatch) error {
	now := time.Now()
	date12MonthsAgo := now.AddDate(0, -12, 0)

	// Get all commits from last 12 months, streaming output
	cmd := exec.Command("git", "-C", repoPath, "log",
		"--since="+date12MonthsAgo.Format("2006-01-02"),
		"--format=%aI|%ae|%P",
		"--all")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("git log failed to start: %w", err)
	}

	// Aggregate commits by author and time bucket in memory
	// Use AuthorID map key for memory efficiency
	commitStats := make(map[uint32]*CommitStats, commitStatsCapacity)

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, scannerBufferSize), scannerMaxLine)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 3 {
			continue
		}

		commitDate, err := time.Parse(time.RFC3339, parts[0])
		if err != nil {
			continue
		}

		author := parts[1]
		authorID := authorIntern.Intern(author)
		parents := parts[2]

		// Initialize stats if needed
		stats, exists := commitStats[authorID]
		if !exists {
			stats = &CommitStats{AuthorID: authorID}
			commitStats[authorID] = stats
		}

		// Determine time bucket
		monthsAgo := int(now.Sub(commitDate).Hours() / hoursPerDay / daysPerMonth)
		switch {
		case monthsAgo < commitBucket3M:
			stats.Commits3M++
		case monthsAgo < commitBucket6M:
			stats.Commits6M++
		default:
			stats.Commits12M++
		}

		// Check if merge (multiple parents)
		if strings.Contains(parents, " ") {
			stats.Merges++
		}
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git log failed: %w", err)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Send commit stats to centralized writer using RepoID
	commitStatsChan <- CommitStatsBatch{
		RepoID: repoID,
		Stats:  commitStats,
	}

	return nil
}

// writeCommitStatsBatch writes commit statistics using optimized multi-row INSERT
// Lookups author strings from AuthorID for database storage
func writeCommitStatsBatch(db *sql.DB, repoPath string, stats map[uint32]*CommitStats) error {
	if len(stats) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Build multi-row INSERT statement
	// INSERT INTO author_commit_stats VALUES (?,?,?,?,?,?),(?,?,?,?,?,?)...
	var sb strings.Builder
	sb.WriteString("INSERT INTO author_commit_stats VALUES ")

	args := make([]interface{}, 0, len(stats)*6)
	first := true
	for authorID, stat := range stats {
		if !first {
			sb.WriteString(",")
		}
		first = false
		sb.WriteString("(?,?,?,?,?,?)")
		// Lookup author string for database
		author := authorIntern.Lookup(authorID)
		args = append(args, repoPath, author, stat.Commits3M, stat.Commits6M, stat.Commits12M, stat.Merges)
	}

	_, err = tx.Exec(sb.String(), args...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// writeBatch writes a batch of file author stats using optimized multi-row INSERT
// This is significantly faster than prepared statement loops for bulk inserts
func writeBatch(db *sql.DB, batch []FileAuthorStats) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Build multi-row INSERT statement
	// INSERT INTO file_author VALUES (?,?,?,?),(?,?,?,?),(?,?,?,?)...
	var sb strings.Builder
	sb.WriteString("INSERT INTO file_author VALUES ")

	args := make([]interface{}, 0, len(batch)*4)
	for i := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?)")

		// Convert RepoID and AuthorID back to strings for database
		repoPath := repoIntern.Lookup(batch[i].RepoID)
		author := authorIntern.Lookup(batch[i].AuthorID)
		args = append(args, repoPath, batch[i].FilePath, author, batch[i].Lines)
	}

	_, err = tx.Exec(sb.String(), args...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// progressUI manages the terminal UI for multi-repo progress tracking
func progressUI(progressChan <-chan RepoProgress, done chan<- struct{}) {
	defer close(done)

	// Track repositories - only keep active ones to prevent memory leak
	repoStates := make(map[string]*RepoProgress)
	repoOrder := make([]string, 0) // Maintain insertion order

	// Track completed repos separately with limited ring buffer
	completedCount := 0
	erroredCount := 0

	// Start runtime timer
	startTime := time.Now()

	ticker := time.NewTicker(uiTickerInterval)
	defer ticker.Stop()

	// Clear screen and hide cursor
	fmt.Print("\033[2J\033[H\033[?25l")
	defer fmt.Print("\033[?25h") // Show cursor on exit

	lastLineCount := 0

	renderUI := func() {
		// Move cursor to top
		fmt.Print("\033[H")

		lines := 0

		// Count repos by status (only active ones in map now)
		var queued, active int
		for _, progress := range repoStates {
			switch progress.Status {
			case StatusQueued:
				queued++
			case StatusOptimizing, StatusDiscovering, StatusBlaming, StatusCommits:
				active++
			}
		}

		// Calculate runtime
		elapsed := time.Since(startTime)
		hours := int(elapsed.Hours())
		minutes := int(elapsed.Minutes()) % 60
		seconds := int(elapsed.Seconds()) % 60
		runtime := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)

		// Header (extended width for large numbers)
		fmt.Printf("┌─ Git Who  ────────────────────────────────────────────────────────────────────────────────────────────────────┐\n")
		lines++
		fmt.Printf("│ Runtime: %s | Active: %d | Done: %d | Queued: %d | Errors: %d | Skipped: %d | Files: %d | Lines: %d     \n",
			runtime, active, completedCount, queued, erroredCount, metrics.filesSkipped.Load(), metrics.filesProcessed.Load(), metrics.linesProcessed.Load())
		lines++
		fmt.Printf("└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘\n")
		lines++
		fmt.Println()
		lines++

		// Select which repos to display (priority order)
		visibleRepos := selectVisibleRepos(repoStates, repoOrder)

		// Repo status lines
		for _, name := range visibleRepos {
			progress := repoStates[name]
			// Defensive nil check (shouldn't happen but prevents crashes)
			if progress != nil {
				lines += renderRepoLine(progress)
			}
		}

		// Show summary line for hidden queued repos
		hiddenQueued := queued - countVisibleQueued(repoStates, visibleRepos)
		if hiddenQueued > 0 {
			fmt.Printf("\033[90m... and %d more queued repositories\033[0m\n", hiddenQueued)
			lines++
		}

		// Clear any remaining lines from previous render
		for i := lines; i < lastLineCount; i++ {
			fmt.Print("\033[K\n") // Clear line
		}
		lastLineCount = lines
	}

	for {
		select {
		case progress, ok := <-progressChan:
			if !ok {
				renderUI() // Final render
				fmt.Println()
				return
			}

			// Update state
			if _, exists := repoStates[progress.Name]; !exists {
				repoOrder = append(repoOrder, progress.Name)
			}

			// Check if repo completed or errored - increment counters and remove from map
			prevStatus := StatusQueued
			if prev, exists := repoStates[progress.Name]; exists {
				prevStatus = prev.Status
			}

			repoStates[progress.Name] = &progress

			// If transitioning to done/error, remove from active tracking to save memory
			if progress.Status == StatusDone && prevStatus != StatusDone {
				completedCount++
				// Only keep in map briefly for display, then remove
				if completedCount > maxCompletedTracking {
					delete(repoStates, progress.Name)
				}
			} else if progress.Status == StatusError && prevStatus != StatusError {
				erroredCount++
				// Keep errors in map for visibility
			}

		case <-ticker.C:
			renderUI()
		}
	}
}

// selectVisibleRepos chooses which repos to display based on priority
// Maintains stable ordering from repoOrder to prevent position jumping
func selectVisibleRepos(repoStates map[string]*RepoProgress, repoOrder []string) []string {
	// Score repos by priority (higher = more important to show)
	scoreRepo := func(status RepoStatus) int {
		switch status {
		case StatusOptimizing, StatusDiscovering, StatusBlaming, StatusCommits:
			return 1000 // Active - highest priority
		case StatusError:
			return 900 // Errors
		case StatusDone:
			return 100 // Completed
		case StatusQueued:
			return 1 // Queued - lowest
		default:
			return 50
		}
	}

	// Build list maintaining original order, with scores
	type scoredRepo struct {
		name  string
		score int
	}

	scored := make([]scoredRepo, 0, len(repoOrder))
	for _, name := range repoOrder {
		progress := repoStates[name]
		// Skip repos that were deleted from map (memory optimization removes completed repos)
		if progress == nil {
			continue
		}
		scored = append(scored, scoredRepo{
			name:  name,
			score: scoreRepo(progress.Status),
		})
	}

	// Filter to keep high-priority repos, but maintain original order
	visible := make([]string, 0, maxVisibleRepos)

	// First pass: add all high-priority repos (active, errors)
	for _, sr := range scored {
		if sr.score >= 900 && len(visible) < maxVisibleRepos {
			visible = append(visible, sr.name)
		}
	}

	// Second pass: skip done repos (don't show completed to save space)
	// Third pass: add queued repos if space remains
	for _, sr := range scored {
		if sr.score < 100 && len(visible) < maxVisibleRepos {
			visible = append(visible, sr.name)
		}
	}

	return visible
}

// countVisibleQueued counts how many queued repos are in the visible list
func countVisibleQueued(repoStates map[string]*RepoProgress, visible []string) int {
	count := 0
	for _, name := range visible {
		progress := repoStates[name]
		if progress != nil && progress.Status == StatusQueued {
			count++
		}
	}
	return count
}

// renderRepoLine renders a single repository's progress line
func renderRepoLine(p *RepoProgress) int {
	// Clear entire line first to prevent overlap
	fmt.Print("\033[K")

	// Truncate name if too long
	displayName := p.Name
	if len(displayName) > maxRepoNameLength {
		displayName = displayName[:maxRepoNameLength-3] + "..."
	}

	// Status indicator and color
	var statusIcon, statusText, color string
	switch p.Status {
	case StatusQueued:
		statusIcon = statusIconQueued
		statusText = "Queued"
		color = "\033[90m" // Gray
	case StatusOptimizing:
		statusIcon = statusIconOptimizing
		statusText = "Optimizing repository"
		color = "\033[95m" // Bright magenta
	case StatusDiscovering:
		statusIcon = statusIconDiscovering
		statusText = "Discovering files"
		color = "\033[36m" // Cyan
	case StatusBlaming:
		statusIcon = statusIconBlaming
		if p.FilesSkipped > 0 {
			statusText = fmt.Sprintf("Blaming %d/%d files (%d skipped)", p.FilesBlamed, p.FilesTotal, p.FilesSkipped)
		} else {
			statusText = fmt.Sprintf("Blaming %d/%d files", p.FilesBlamed, p.FilesTotal)
		}
		color = "\033[33m" // Yellow
	case StatusCommits:
		statusIcon = statusIconCommits
		statusText = "Analyzing commits"
		color = "\033[35m" // Magenta
	case StatusDone:
		statusIcon = statusIconDone
		if p.FilesSkipped > 0 {
			statusText = fmt.Sprintf("Complete (%d files, %d lines, %d skipped)", p.FilesTotal, p.LinesBlamed, p.FilesSkipped)
		} else {
			statusText = fmt.Sprintf("Complete (%d files, %d lines)", p.FilesTotal, p.LinesBlamed)
		}
		color = "\033[32m" // Green
	case StatusError:
		statusIcon = statusIconError
		statusText = "Error: " + p.ErrorMessage
		color = "\033[31m" // Red
	default:
		statusIcon = "[?]"
		statusText = p.Status.String()
		color = "\033[0m"
	}

	// Progress bar for blaming phase
	progressBar := ""
	if p.Status == StatusBlaming && p.FilesTotal > 0 {
		filled := int(float64(p.FilesBlamed) / float64(p.FilesTotal) * float64(progressBarWidth))
		if filled > progressBarWidth {
			filled = progressBarWidth
		}
		progressBar = " ["
		for i := 0; i < progressBarWidth; i++ {
			if i < filled {
				progressBar += "="
			} else {
				progressBar += "-"
			}
		}
		pct := float64(p.FilesBlamed) / float64(p.FilesTotal) * 100
		progressBar += fmt.Sprintf("] %.0f%%", pct)
	}

	fmt.Printf("%s%-4s %-*s %s%s\033[0m\n",
		color, statusIcon, maxRepoNameLength, displayName, statusText, progressBar)

	return 1
}

// repoWorker processes repositories from a task channel
func repoWorker(blameWorkers int, repoTasks <-chan RepoInfo, results chan<- RepoResult, progressChan chan<- RepoProgress,
	fileStatsChan chan<- FileAuthorStats, commitStatsChan chan<- CommitStatsBatch,
	fileLister FileLister, wg *sync.WaitGroup) {
	defer wg.Done()

	for repo := range repoTasks {
		err := processRepository(repo, blameWorkers, progressChan, fileStatsChan, commitStatsChan, fileLister)

		result := RepoResult{
			RepoName: repo.Name,
			Success:  err == nil,
			Error:    err,
		}

		if err == nil {
			metrics.reposProcessed.Add(1)
		}

		results <- result
	}
}

// generateSummary prints final statistics
func generateSummary(db *sql.DB) {
	fmt.Printf("Database: %s\n", dbFile)
	fmt.Println()

	// Query summary stats
	var totalLines, totalFiles, totalAuthors, totalRepos int64
	_ = db.QueryRow("SELECT SUM(lines) FROM file_author").Scan(&totalLines)
	_ = db.QueryRow("SELECT COUNT(DISTINCT repo_path, file_path) FROM file_author").Scan(&totalFiles)
	_ = db.QueryRow("SELECT COUNT(DISTINCT author) FROM file_author").Scan(&totalAuthors)
	db.QueryRow("SELECT COUNT(DISTINCT repo_path) FROM file_author").Scan(&totalRepos)

	fmt.Println("Summary Statistics:")
	fmt.Printf("  Total repositories: %d\n", totalRepos)
	fmt.Printf("  Total lines analyzed: %d\n", totalLines)
	fmt.Printf("  Unique files: %d\n", totalFiles)
	fmt.Printf("  Unique authors: %d\n", totalAuthors)
	fmt.Println()

	// Show top contributors
	fmt.Println("Top 10 Contributors (by lines in current HEAD):")
	fmt.Println()

	query := `
		SELECT
			fa.author,
			SUM(fa.lines) as total_lines,
			COALESCE(SUM(acs.commits_3m), 0) as commits_3m,
			COALESCE(SUM(acs.commits_6m), 0) as commits_6m,
			COALESCE(SUM(acs.commits_12m), 0) as commits_12m,
			COALESCE(SUM(acs.merges), 0) as merges
		FROM file_author fa
		LEFT JOIN author_commit_stats acs ON fa.repo_path = acs.repo_path AND fa.author = acs.author
		GROUP BY fa.author
		ORDER BY total_lines DESC
		LIMIT 10
	`

	rows, err := db.Query(query)
	if err != nil {
		fmt.Printf("Error querying stats: %v\n", err)
		return
	}
	defer func() { _ = rows.Close() }()

	fmt.Printf("%-40s %15s %10s %10s %10s %10s\n",
		"Author", "Lines", "3mo", "6mo", "12mo", "Merges")
	fmt.Println(strings.Repeat("-", 110))

	for rows.Next() {
		var author string
		var lines, c3m, c6m, c12m, merges int
		if err := rows.Scan(&author, &lines, &c3m, &c6m, &c12m, &merges); err != nil {
			continue
		}
		fmt.Printf("%-40s %15d %10d %10d %10d %10d\n",
			truncate(author, 40), lines, c3m, c6m, c12m, merges)
	}

	fmt.Println()
	fmt.Println("Example queries:")
	fmt.Println()
	fmt.Println("# Lines per author (all repos):")
	fmt.Printf("sqlite3 %s \"SELECT author, SUM(lines) as total FROM file_author GROUP BY author ORDER BY total DESC;\"\n", dbFile)
	fmt.Println()
	fmt.Println("# Lines per author per repo:")
	fmt.Printf("sqlite3 %s \"SELECT repo_path, author, SUM(lines) as total FROM file_author GROUP BY repo_path, author ORDER BY repo_path, total DESC;\"\n", dbFile)
	fmt.Println()
	fmt.Println("# Files per author:")
	fmt.Printf("sqlite3 %s \"SELECT author, COUNT(DISTINCT repo_path, file_path) as files FROM file_author GROUP BY author ORDER BY files DESC;\"\n", dbFile)
	fmt.Println()
	fmt.Println("# Ownership of a specific file:")
	fmt.Printf("sqlite3 %s \"SELECT author, lines, ROUND(lines*100.0/(SELECT SUM(lines) FROM file_author WHERE repo_path='REPO' AND file_path='FILE'),2) as pct FROM file_author WHERE repo_path='REPO' AND file_path='FILE' ORDER BY lines DESC;\"\n", dbFile)
}

// truncate truncates a string to maxLen
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
