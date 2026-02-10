package core

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
)

// RunAnalyzeCommand runs the full analyze pipeline
func RunAnalyzeCommand(targetPath string, fileListMode string) {
	var fileLister FileLister
	switch fileListMode {
	case "git":
		fileLister = GitLsTreeLister{}
	case "fs":
		fileLister = FSWalkerLister{}
	}

	if Verbose {
		fmt.Println("=== Git Who - Repository Contribution Analyzer (Optimized Go Edition) ===")
		fmt.Println()
		fmt.Printf("File listing mode: %s\n", fileListMode)
		fmt.Println()
	}

	debug.SetGCPercent(GCPercent)
	debug.SetMemoryLimit(MemoryLimit)

	if Verbose {
		fmt.Println("Memory optimizations: GC=50%, Limit=4GB, Streaming I/O, Interned paths")
		fmt.Println()
	}

	repos, err := FindGitRepos(targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if Verbose {
		fmt.Printf("Found %d git repository(ies) to analyze\n", len(repos))
		for _, repo := range repos {
			fmt.Printf("  - %s (%s)\n", repo.Name, repo.Path)
		}
		fmt.Println()
	}

	repos = PreflightCountFiles(repos, fileLister)

	if Verbose {
		fmt.Println("Optimized processing order (smallest to largest):")
		for i, repo := range repos {
			fmt.Printf("  %2d. %-30s (%d files)\n", i+1, repo.Name, repo.FileCount)
		}
		fmt.Println()
	}

	db, err := CreateDatabase()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	numCPU := runtime.NumCPU()
	blameWorkers := numCPU * BlameWorkersMultiplier
	repoWorkers := numCPU / RepoWorkersDivisor
	if repoWorkers < 1 {
		repoWorkers = 1
	}
	if repoWorkers > MaxRepoWorkers {
		repoWorkers = MaxRepoWorkers
	}
	if Verbose {
		fmt.Printf("Using %d CPU cores with %d repo workers and %d blame workers per repo\n",
			numCPU, repoWorkers, blameWorkers)
		fmt.Println()
	}

	repoTasks := make(chan RepoInfo, RepoTaskBuffer)
	repoResults := make(chan RepoResult, len(repos))
	progressChanSize := ProgressChanDefault
	if len(repos) < ProgressChanThreshold {
		progressChanSize = len(repos) * ProgressChanMultiplier
	}
	progressChan := make(chan RepoProgress, progressChanSize)

	fileStatsChan := make(chan FileAuthorStats, ContributionBuffer)
	commitStatsChan := make(chan CommitStatsBatch, CommitStatsBuffer)

	uiDone := make(chan struct{})
	go ProgressUI(progressChan, uiDone)

	for _, repo := range repos {
		progressChan <- RepoProgress{
			Name:   repo.Name,
			Status: StatusQueued,
		}
	}

	dbWriterDone := make(chan struct{})
	go CentralizedDatabaseWriter(db, fileStatsChan, commitStatsChan, dbWriterDone)

	var repoWg sync.WaitGroup
	for i := 0; i < repoWorkers; i++ {
		repoWg.Add(1)
		go RepoWorker(blameWorkers, repoTasks, repoResults, progressChan, fileStatsChan, commitStatsChan, fileLister, &repoWg)
	}

	go func() {
		for _, repo := range repos {
			repoTasks <- repo
		}
		close(repoTasks)
	}()

	go func() {
		repoWg.Wait()
		close(repoResults)
		close(fileStatsChan)
		close(commitStatsChan)
	}()

	successCount := 0
	errorCount := 0
	for result := range repoResults {
		if result.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	close(progressChan)
	<-uiDone

	<-dbWriterDone

	fmt.Println()
	fmt.Println("=== Analysis Complete ===")
	fmt.Printf("Successfully processed %d/%d repositories (%d errors)\n",
		successCount, len(repos), errorCount)
	if GlobalMetrics.FilesSkipped.Load() > 0 {
		fmt.Printf("Files skipped due to timeout: %d\n", GlobalMetrics.FilesSkipped.Load())
	}
	fmt.Println()
	GenerateSummary(db)
}
