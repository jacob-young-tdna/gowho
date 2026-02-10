package core

import "time"

// CLI Args
const (
	FileListModeDefault = "git"
)

// Memory Management
const (
	GCPercent   = 50
	MemoryLimit = 4 * 1024 * 1024 * 1024
)

// Parallelism: Worker Pool Configuration
const (
	BlameWorkersMultiplier         = 2
	RepoWorkersDivisor             = 2
	MaxRepoWorkers                 = 4
	ScanConcurrencyMultiplier      = 4
	PreflightConcurrencyMultiplier = 2
)

// Channel Buffers: CSP Communication
// Increased buffer sizes reduce goroutine blocking and context switch overhead
// Larger buffers allow producers and consumers to run more independently, reducing synchronization cost
const (
	FileTaskBuffer         = 2000
	ContributionBuffer     = 5000
	RepoTaskBuffer         = 50
	CommitStatsBuffer      = 500
	RepoScanBuffer         = 100
	ProgressChanDefault    = 1000
	ProgressChanThreshold  = 100
	ProgressChanMultiplier = 10
)

// Capacity Hints: Pre-allocation for Memory Efficiency
const (
	RepoInternCapacity   = 2000
	AuthorInternCapacity = 1024
	AuthorCountsCapacity = 32
	CommitStatsCapacity  = 256
	FileListCapacity     = 1024
	FileListQueueBuffer  = 1000
)

// Timeouts and Intervals
const (
	GitBlameTimeout        = 10 * time.Second
	GitLogTimeout          = 5 * time.Minute
	BatchFlushInterval     = 500 * time.Millisecond
	ProgressReportInterval = 200 * time.Millisecond
	UITickerInterval       = 100 * time.Millisecond
)

// Scanner: Buffer Sizes for Streaming I/O
const (
	ScannerBufferSize = 128 * 1024
	ScannerMaxLine    = 1024 * 1024
)

// Worker Buffer Reuse: Per-worker buffer sizes to eliminate allocation churn
// Each worker goroutine maintains its own buffers to avoid contention
//
// Memory cost per worker = WorkerScannerBufferSize + (WorkerAuthorMapCapacity * ~24 bytes)
// Total memory cost = blameWorkers * (WorkerScannerBufferSize + WorkerAuthorMapCapacity * 24)
const (
	WorkerScannerBufferSize = 256 * 1024
	WorkerAuthorMapCapacity = 128
)

// Time Calculations: Commit Bucketing
const (
	HoursPerDay    = 24
	DaysPerMonth   = 30
	CommitBucket3M = 3
	CommitBucket6M = 6
)

// UI: Display Configuration
const (
	MaxVisibleRepos      = 15
	MaxCompletedTracking = 50
	MaxRepoNameLength    = 35
	ProgressBarWidth     = 20
)

// UI status indicators (ASCII only)
const (
	StatusIconQueued      = "[-]"
	StatusIconOptimizing  = "[⚡]"
	StatusIconDiscovering = "[*]"
	StatusIconBlaming     = "[>]"
	StatusIconCommits     = "[=]"
	StatusIconDone        = "[✓]"
	StatusIconError       = "[X]"
)
