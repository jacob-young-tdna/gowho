package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// LoadOptimizationMarker reads the .gowho-optimized marker file
func LoadOptimizationMarker(repoPath string) (*OptimizationMarker, error) {
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

// SaveOptimizationMarker writes the .gowho-optimized marker file
func SaveOptimizationMarker(repoPath string, mode OptimizationMode) error {
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
		"gowho-optimized")

	return os.WriteFile(markerPath, []byte(content), 0644)
}

// ShouldSkipOptimization checks if we should skip optimization based on marker
func ShouldSkipOptimization(repoPath string, requestedMode OptimizationMode) bool {
	marker, err := LoadOptimizationMarker(repoPath)
	if err != nil {
		return false
	}

	if marker.Mode >= requestedMode {
		return time.Since(marker.Timestamp) <= 30*24*time.Hour
	}

	return false
}

// RunOptimizeCommand runs the optimize command (separate from analyze)
// Optimizes repos in REVERSE order (largest first) using all CPU cores
func RunOptimizeCommand(targetPath string, optimizationMode OptimizationMode, forceOptimize bool, jobs int) {
	if Verbose {
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

	repos, err := FindGitRepos(targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if Verbose {
		fmt.Printf("Found %d git repository(ies) to optimize\n\n", len(repos))
	}

	fileLister := GitLsTreeLister{}
	repos = PreflightCountFiles(repos, fileLister)

	sort.Slice(repos, func(i, j int) bool {
		return repos[i].FileCount > repos[j].FileCount
	})

	if Verbose {
		fmt.Println("Optimization order (largest to smallest):")
		for i, repo := range repos {
			fmt.Printf("  %2d. %-30s (%d files)\n", i+1, repo.Name, repo.FileCount)
		}
		fmt.Println()
	}

	progressChanSize := ProgressChanDefault
	if len(repos) < ProgressChanThreshold {
		progressChanSize = len(repos) * ProgressChanMultiplier
	}
	progressChan := make(chan RepoProgress, progressChanSize)

	uiDone := make(chan struct{})
	go ProgressUI(progressChan, uiDone)

	for _, repo := range repos {
		progressChan <- RepoProgress{
			Name:   repo.Name,
			Status: StatusQueued,
		}
	}

	type OptimizeResult struct {
		RepoName string
		Success  bool
		Error    error
		Skipped  bool
	}

	results := make(chan OptimizeResult, len(repos))
	repoTasks := make(chan RepoInfo, len(repos))

	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for repo := range repoTasks {
				progressChan <- RepoProgress{
					Name:   repo.Name,
					Status: StatusOptimizing,
				}

				if !forceOptimize && ShouldSkipOptimization(repo.Path, optimizationMode) {
					if Verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] Skipping %s (already optimized)\n", workerID, repo.Name)
					}
					progressChan <- RepoProgress{
						Name:   repo.Name,
						Status: StatusDone,
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: true, Skipped: true}
					continue
				}

				if !ShouldOptimizeRepo(repo.Path) && !forceOptimize {
					if Verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] Skipping %s (already optimal)\n", workerID, repo.Name)
					}
					progressChan <- RepoProgress{
						Name:   repo.Name,
						Status: StatusDone,
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: true, Skipped: true}
					continue
				}

				var err error
				switch optimizationMode {
				case OptimizeQuick:
					err = QuickOptimizeRepo(repo.Path, repo.Name)
				case OptimizeFull:
					err = FullOptimizeRepo(repo.Path, repo.Name)
				}

				if err != nil {
					if Verbose {
						fmt.Fprintf(os.Stderr, "[Worker %d] ERROR optimizing %s: %v\n", workerID, repo.Name, err)
					}
					progressChan <- RepoProgress{
						Name:         repo.Name,
						Status:       StatusError,
						ErrorMessage: err.Error(),
					}
					results <- OptimizeResult{RepoName: repo.Name, Success: false, Error: err}
				} else {
					if err := SaveOptimizationMarker(repo.Path, optimizationMode); err != nil {
						if Verbose {
							fmt.Fprintf(os.Stderr, "[Worker %d] Warning: failed to save marker for %s: %v\n", workerID, repo.Name, err)
						}
					}
					if Verbose {
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

	go func() {
		for _, repo := range repos {
			repoTasks <- repo
		}
		close(repoTasks)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

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

	close(progressChan)
	<-uiDone

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
