package core

import (
	"strings"
	"sync/atomic"
	"time"
)

// RepoStatus represents the processing state of a repository
// Using uint8 enum instead of string saves 15 bytes per RepoProgress (16 bytes -> 1 byte)
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
// Fields ordered for optimal cache alignment: large -> small
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
// Fields ordered for optimal cache alignment: large -> small
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
// on multi-core systems.
type Metrics struct {
	FilesProcessed atomic.Int64
	_              [56]byte // Cache line padding (64 - 8 = 56)

	LinesProcessed atomic.Int64
	_              [56]byte // Cache line padding

	Errors atomic.Int64
	_      [56]byte // Cache line padding

	ReposProcessed atomic.Int64
	_              [56]byte // Cache line padding

	FilesSkipped atomic.Int64 // Files skipped due to timeout or other issues
	_            [56]byte     // Cache line padding
}

// RepoInfo represents a git repository to process
type RepoInfo struct {
	Path      string // Absolute path to the repository
	Name      string // Name for display/identification
	FileCount int    // Number of files to process (from pre-flight count)
}

// Text file extensions for fast-path detection
var TextExtensions = map[string]bool{
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

// GetCodeExtensions returns a slice of code file extensions for gocodewalker
func GetCodeExtensions() []string {
	extensions := make([]string, 0, len(TextExtensions))
	for ext := range TextExtensions {
		extensions = append(extensions, strings.TrimPrefix(ext, "."))
	}
	return extensions
}

// FileLister abstracts how we enumerate code files for blame
type FileLister interface {
	List(repoPath string) ([]string, error)
	Count(repoPath string) (int, error)
}

// OptimizationMarker tracks optimization state per repository
type OptimizationMarker struct {
	Timestamp    time.Time
	Mode         OptimizationMode
	GowhoVersion string
}

// Truncate truncates a string to maxLen
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
