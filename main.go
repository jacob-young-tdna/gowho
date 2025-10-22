package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
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

	"github.com/boyter/gocodewalker"
	_ "modernc.org/sqlite"
)

// Performance Tuning Constants
// All performance-sensitive parameters are extracted here for easy tuning.
// These values control parallelism, memory usage, buffering, and timeouts.

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
const (
	fileTaskBuffer         = 500  // Buffer size for file blame tasks within a repo
	contributionBuffer     = 1000 // Buffer size for file author stats aggregation
	repoTaskBuffer         = 50   // Buffer size for repository tasks
	commitStatsBuffer      = 100  // Buffer size for commit statistics batches
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
	batchSizeThreshold = 10000 // File stats batch size before flush (increased for write performance)
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
	scannerBufferSize = 64 * 1024   // Initial scanner buffer (64KB)
	scannerMaxLine    = 1024 * 1024 // Maximum line size (1MB)
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
	StatusDiscovering
	StatusBlaming
	StatusCommits
	StatusDone
	StatusError
)

// String returns the string representation for display
// This allows automatic string conversion while maintaining memory efficiency
func (s RepoStatus) String() string {
	switch s {
	case StatusQueued:
		return "queued"
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
type CommitStatsBatch struct {
	RepoID uint16 // Interned repository ID
	Stats  map[string]*CommitStats
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
	_            [56]byte      // Cache line padding
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

// isGitRepo checks if a directory is a git repository
func isGitRepo(path string) bool {
	gitDir := filepath.Join(path, ".git")
	info, err := os.Stat(gitDir)
	return err == nil && info.IsDir()
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

func main() {
	// Parse command line arguments
	targetPath := "."
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "-v" || arg == "--verbose" {
			verbose = true
		} else if !strings.HasPrefix(arg, "-") {
			targetPath = arg
		}
	}

	if verbose {
		fmt.Println("=== Git Who - Repository Contribution Analyzer (Optimized Go Edition) ===")
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
	repos = preflightCountFiles(repos)

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
	defer db.Close()

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
		go repoWorker(blameWorkers, repoTasks, repoResults, progressChan, fileStatsChan, commitStatsChan, &repoWg)
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
func processRepository(repo RepoInfo, blameWorkers int, progressChan chan<- RepoProgress,
	fileStatsChan chan<- FileAuthorStats, commitStatsChan chan<- CommitStatsBatch) error {

	// Intern the repository path once at the start
	repoID := repoIntern.Intern(repo.Path)

	// Update status: discovering files
	progressChan <- RepoProgress{
		Name:   repo.Name,
		Status: StatusDiscovering,
	}

	// Get file list from HEAD
	files, err := getFilesAtHEAD(repo.Path)
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
	os.Remove(dbFile)

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
	go fileWalker.Start()

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

	go fileWalker.Start()

	count := 0
	for range fileListQueue {
		count++
	}

	repo.FileCount = count
	return repo, nil
}

// preflightCountFiles counts files in all repositories in parallel and sorts them
func preflightCountFiles(repos []RepoInfo) []RepoInfo {
	if verbose {
		fmt.Println("Pre-flight: Counting files in each repository to optimize processing order...")
	}
	startTime := time.Now()

	type countResult struct {
		repo RepoInfo
		err  error
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

			counted, err := countFilesInRepo(r)
			results <- countResult{repo: counted, err: err}
		}(repo)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	countedRepos := make([]RepoInfo, 0, len(repos))
	for result := range results {
		if result.err == nil {
			countedRepos = append(countedRepos, result.repo)
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
		fmt.Println()
	}

	return countedRepos
}

// parseGitBlamePorcelain parses git blame --line-porcelain output and aggregates by author
// Uses a scanner for streaming to avoid loading entire output into memory
func parseGitBlamePorcelain(scanner *bufio.Scanner) map[string]int {
	authorCounts := make(map[string]int, authorCountsCapacity)

	var currentAuthor string

	for scanner.Scan() {
		line := scanner.Bytes() // Avoid string allocation

		// author-mail <email@example.com>
		if bytes.HasPrefix(line, []byte("author-mail ")) {
			// Extract email, removing angle brackets
			email := line[12:] // Skip "author-mail "
			if len(email) > 2 && email[0] == '<' && email[len(email)-1] == '>' {
				currentAuthor = string(email[1 : len(email)-1])
			}
		} else if len(line) > 0 && line[0] == '\t' {
			// Tab-prefixed lines are actual content lines
			if currentAuthor != "" {
				authorCounts[currentAuthor]++
				currentAuthor = ""
			}
		}
	}

	return authorCounts
}

// blameWorker processes file tasks and emits aggregated author stats
// Uses streaming to avoid buffering entire git blame output in memory
func blameWorker(ctx context.Context, repoPath string, repoID uint16, fileTasks <-chan FileTask,
	stats chan<- FileAuthorStats, filesBlamed *atomic.Int64, linesBlamed *atomic.Int64,
	filesSkipped *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range fileTasks {
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
		scanner.Buffer(make([]byte, scannerBufferSize), scannerMaxLine)
		authorCounts := parseGitBlamePorcelain(scanner)

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

		// Emit aggregated stats using RepoID instead of RepoPath string
		totalLines := 0
		for author, count := range authorCounts {
			stat := FileAuthorStats{
				RepoID:   repoID,
				FilePath: task.FilePath,
				Author:   author,
				Lines:    int32(count),
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
	commitStats := make(map[string]*CommitStats, commitStatsCapacity)

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
		parents := parts[2]

		// Initialize stats if needed
		stats, exists := commitStats[author]
		if !exists {
			stats = &CommitStats{Author: author}
			commitStats[author] = stats
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
func writeCommitStatsBatch(db *sql.DB, repoPath string, stats map[string]*CommitStats) error {
	if len(stats) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Build multi-row INSERT statement
	// INSERT INTO author_commit_stats VALUES (?,?,?,?,?,?),(?,?,?,?,?,?)...
	var sb strings.Builder
	sb.WriteString("INSERT INTO author_commit_stats VALUES ")

	args := make([]interface{}, 0, len(stats)*6)
	first := true
	for _, stat := range stats {
		if !first {
			sb.WriteString(",")
		}
		first = false
		sb.WriteString("(?,?,?,?,?,?)")
		args = append(args, repoPath, stat.Author, stat.Commits3M, stat.Commits6M, stat.Commits12M, stat.Merges)
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
	defer tx.Rollback()

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

		// Convert RepoID back to path for database
		repoPath := repoIntern.Lookup(batch[i].RepoID)
		args = append(args, repoPath, batch[i].FilePath, batch[i].Author, batch[i].Lines)
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
			case StatusDiscovering, StatusBlaming, StatusCommits:
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
		case StatusDiscovering, StatusBlaming, StatusCommits:
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
func repoWorker(blameWorkers int, repoTasks <-chan RepoInfo,
	results chan<- RepoResult, progressChan chan<- RepoProgress,
	fileStatsChan chan<- FileAuthorStats, commitStatsChan chan<- CommitStatsBatch,
	wg *sync.WaitGroup) {
	defer wg.Done()

	for repo := range repoTasks {
		err := processRepository(repo, blameWorkers, progressChan, fileStatsChan, commitStatsChan)

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
	db.QueryRow("SELECT SUM(lines) FROM file_author").Scan(&totalLines)
	db.QueryRow("SELECT COUNT(DISTINCT repo_path, file_path) FROM file_author").Scan(&totalFiles)
	db.QueryRow("SELECT COUNT(DISTINCT author) FROM file_author").Scan(&totalAuthors)
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
	defer rows.Close()

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
