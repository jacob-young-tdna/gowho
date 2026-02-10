package core

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BlameWorkerState holds reusable buffers for a blame worker
// Reusing buffers eliminates per-file allocation churn (640MB for 10K files)
type BlameWorkerState struct {
	scannerBuf   []byte
	authorCounts map[uint32]int
}

func NewBlameWorkerState() *BlameWorkerState {
	return &BlameWorkerState{
		scannerBuf:   make([]byte, WorkerScannerBufferSize),
		authorCounts: make(map[uint32]int, WorkerAuthorMapCapacity),
	}
}

func (s *BlameWorkerState) Reset() {
	for k := range s.authorCounts {
		delete(s.authorCounts, k)
	}
}

// ParseGitBlamePorcelain parses git blame --line-porcelain output and aggregates by author
// Uses a scanner for streaming to avoid loading entire output into memory
// Returns map keyed by interned AuthorID instead of email string
func ParseGitBlamePorcelain(scanner *bufio.Scanner, authorCounts map[uint32]int) (map[uint32]int, error) {
	var currentAuthorID uint32

	for scanner.Scan() {
		line := scanner.Bytes()

		if bytes.HasPrefix(line, []byte("author-mail ")) {
			email := line[12:]
			if len(email) > 2 && email[0] == '<' && email[len(email)-1] == '>' {
				emailBytes := email[1 : len(email)-1]
				currentAuthorID = AuthorInterns.InternBytes(emailBytes)
			}
		} else if len(line) > 0 && line[0] == '\t' {
			if currentAuthorID != 0 {
				authorCounts[currentAuthorID]++
				currentAuthorID = 0
			}
		}
	}

	return authorCounts, scanner.Err()
}

// BlameWorker processes file tasks and emits aggregated author stats
func BlameWorker(ctx context.Context, repoPath string, repoID uint16, fileTasks <-chan FileTask,
	stats chan<- FileAuthorStats, filesBlamed *atomic.Int64, linesBlamed *atomic.Int64,
	filesSkipped *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	state := NewBlameWorkerState()

	for task := range fileTasks {
		state.Reset()
		select {
		case <-ctx.Done():
			return
		default:
		}

		blameCtx, cancel := context.WithTimeout(ctx, GitBlameTimeout)

		cmd := exec.CommandContext(blameCtx, "git", "-C", repoPath, "blame", "--line-porcelain", "-w", "HEAD", "--", task.FilePath)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			GlobalMetrics.Errors.Add(1)
			GlobalMetrics.FilesProcessed.Add(1)
			filesBlamed.Add(1)
			cancel()
			continue
		}

		if err := cmd.Start(); err != nil {
			GlobalMetrics.Errors.Add(1)
			GlobalMetrics.FilesProcessed.Add(1)
			filesBlamed.Add(1)
			cancel()
			continue
		}

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(state.scannerBuf, ScannerMaxLine)
		authorCounts, scanErr := ParseGitBlamePorcelain(scanner, state.authorCounts)

		if err := cmd.Wait(); err != nil {
			if blameCtx.Err() == context.DeadlineExceeded {
				GlobalMetrics.FilesSkipped.Add(1)
				GlobalMetrics.FilesProcessed.Add(1)
				filesBlamed.Add(1)
				filesSkipped.Add(1)
				if Verbose {
					fmt.Fprintf(os.Stderr, "Warning: git blame timed out after %v for file: %s\n", GitBlameTimeout, task.FilePath)
				}
			} else {
				GlobalMetrics.Errors.Add(1)
				GlobalMetrics.FilesProcessed.Add(1)
				filesBlamed.Add(1)
			}
			cancel()
			continue
		}

		if scanErr != nil {
			GlobalMetrics.Errors.Add(1)
			GlobalMetrics.FilesProcessed.Add(1)
			filesBlamed.Add(1)
			cancel()
			continue
		}

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

		GlobalMetrics.LinesProcessed.Add(int64(totalLines))
		GlobalMetrics.FilesProcessed.Add(1)
		filesBlamed.Add(1)
		linesBlamed.Add(int64(totalLines))
		cancel()
	}
}

// ProcessRepository analyzes a single git repository
func ProcessRepository(repo RepoInfo, blameWorkers int,
	progressChan chan<- RepoProgress, fileStatsChan chan<- FileAuthorStats,
	commitStatsChan chan<- CommitStatsBatch, fileLister FileLister) error {

	repoID := RepoInterns.Intern(repo.Path)

	progressChan <- RepoProgress{
		Name:   repo.Name,
		Status: StatusDiscovering,
	}

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

	progressChan <- RepoProgress{
		Name:        repo.Name,
		Status:      StatusBlaming,
		FilesTotal:  int32(totalFiles),
		FilesBlamed: 0,
	}

	fileTaskChan := make(chan FileTask, FileTaskBuffer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var filesBlamed atomic.Int64
	var linesBlamed atomic.Int64
	var filesSkipped atomic.Int64

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(ProgressReportInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				select {
				case progressChan <- RepoProgress{
					Name:         repo.Name,
					Status:       StatusBlaming,
					FilesTotal:   int32(totalFiles),
					FilesBlamed:  int32(filesBlamed.Load()),
					FilesSkipped: int32(filesSkipped.Load()),
					LinesBlamed:  linesBlamed.Load(),
				}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var blameWg sync.WaitGroup
	for i := 0; i < blameWorkers; i++ {
		blameWg.Add(1)
		go BlameWorker(ctx, repo.Path, repoID, fileTaskChan, fileStatsChan, &filesBlamed, &linesBlamed, &filesSkipped, &blameWg)
	}

	for _, file := range files {
		fileTaskChan <- FileTask{
			RepoPath: repo.Path,
			FilePath: file,
		}
	}
	close(fileTaskChan)

	blameWg.Wait()
	cancel()

	<-progressDone

	progressChan <- RepoProgress{
		Name:        repo.Name,
		Status:      StatusCommits,
		FilesTotal:  int32(totalFiles),
		FilesBlamed: int32(totalFiles),
		LinesBlamed: linesBlamed.Load(),
	}

	if err := ProcessCommitStats(context.Background(), repo.Path, repoID, commitStatsChan); err != nil {
		progressChan <- RepoProgress{
			Name:         repo.Name,
			Status:       StatusError,
			ErrorMessage: err.Error(),
		}
		return fmt.Errorf("failed to process commit stats: %w", err)
	}

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

// ProcessCommitStats analyzes commit history and buckets by time
func ProcessCommitStats(ctx context.Context, repoPath string, repoID uint16, commitStatsChan chan<- CommitStatsBatch) error {
	now := time.Now()
	date12MonthsAgo := now.AddDate(0, -12, 0)

	logCtx, logCancel := context.WithTimeout(ctx, GitLogTimeout)
	defer logCancel()
	cmd := exec.CommandContext(logCtx, "git", "-C", repoPath, "log",
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

	commitStats := make(map[uint32]*CommitStats, CommitStatsCapacity)

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, ScannerBufferSize), ScannerMaxLine)

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
		authorID := AuthorInterns.Intern(author)
		parents := parts[2]

		stats, exists := commitStats[authorID]
		if !exists {
			stats = &CommitStats{AuthorID: authorID}
			commitStats[authorID] = stats
		}

		monthsAgo := int(now.Sub(commitDate).Hours() / HoursPerDay / DaysPerMonth)
		switch {
		case monthsAgo < CommitBucket3M:
			stats.Commits3M++
		case monthsAgo < CommitBucket6M:
			stats.Commits6M++
		default:
			stats.Commits12M++
		}

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

	commitStatsChan <- CommitStatsBatch{
		RepoID: repoID,
		Stats:  commitStats,
	}

	return nil
}

// RepoWorker processes repositories from a task channel
func RepoWorker(blameWorkers int, repoTasks <-chan RepoInfo, results chan<- RepoResult, progressChan chan<- RepoProgress,
	fileStatsChan chan<- FileAuthorStats, commitStatsChan chan<- CommitStatsBatch,
	fileLister FileLister, wg *sync.WaitGroup) {
	defer wg.Done()

	for repo := range repoTasks {
		err := ProcessRepository(repo, blameWorkers, progressChan, fileStatsChan, commitStatsChan, fileLister)

		result := RepoResult{
			RepoName: repo.Name,
			Success:  err == nil,
			Error:    err,
		}

		if err == nil {
			GlobalMetrics.ReposProcessed.Add(1)
		}

		results <- result
	}
}
