package core

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/boyter/gocodewalker"
)

// GitLsTreeLister uses git ls-tree to enumerate files at HEAD
type GitLsTreeLister struct{}

func (GitLsTreeLister) List(repoPath string) ([]string, error) {
	cmd := exec.Command("git", "-C", repoPath, "ls-tree", "-r", "-z", "--name-only", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr := string(exitErr.Stderr)
			if strings.Contains(stderr, "Not a valid object name") {
				return []string{}, nil
			}
			return nil, fmt.Errorf("git ls-tree failed: %w (stderr: %s)", err, stderr)
		}
		return nil, fmt.Errorf("git ls-tree failed: %w", err)
	}

	raw := bytes.Split(out, []byte{0})
	files := make([]string, 0, len(raw))
	for _, b := range raw {
		if len(b) == 0 {
			continue
		}
		p := string(b)
		if TextExtensions[strings.ToLower(filepath.Ext(p))] {
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
				if TextExtensions[strings.ToLower(filepath.Ext(p))] {
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
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr := string(exitErr.Stderr)
			if strings.Contains(stderr, "Not a valid object name") {
				return 0, nil
			}
		}
		return 0, err
	}
	return count, nil
}

// FSWalkerLister uses gocodewalker (filesystem walk) to enumerate files
type FSWalkerLister struct{}

func (FSWalkerLister) List(repoPath string) ([]string, error) {
	return GetFilesAtHEAD(repoPath)
}

func (FSWalkerLister) Count(repoPath string) (int, error) {
	ri := RepoInfo{Path: repoPath}
	counted, err := CountFilesInRepo(ri)
	if err != nil {
		return 0, err
	}
	return counted.FileCount, nil
}

// IsGitRepo checks if a directory is a git repository
func IsGitRepo(path string) bool {
	gitDir := filepath.Join(path, ".git")
	info, err := os.Stat(gitDir)
	return err == nil && info.IsDir()
}

// ShouldOptimizeRepo checks if a repository needs optimization for blame performance
func ShouldOptimizeRepo(repoPath string) bool {
	commitGraphPath := filepath.Join(repoPath, ".git", "objects", "info", "commit-graph")
	if _, err := os.Stat(commitGraphPath); os.IsNotExist(err) {
		if Verbose {
			fmt.Fprintf(os.Stderr, "Repo needs optimization: %s (missing commit-graph)\n", filepath.Base(repoPath))
		}
		return true
	}

	packDir := filepath.Join(repoPath, ".git", "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err == nil && len(packFiles) > 1 {
		if Verbose {
			fmt.Fprintf(os.Stderr, "Repo needs optimization: %s (%d packfiles, should be 1)\n", filepath.Base(repoPath), len(packFiles))
		}
		return true
	}

	criticalConfigs := map[string]string{
		"core.commitGraph":         "true",
		"core.deltaBaseCacheLimit": "512m",
		"pack.useBitmaps":          "true",
	}

	for key, expectedValue := range criticalConfigs {
		cmd := exec.Command("git", "-C", repoPath, "config", "--get", key)
		output, err := cmd.Output()
		if err != nil || strings.TrimSpace(string(output)) != expectedValue {
			if Verbose {
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

// QuickOptimizeRepo applies lightweight optimizations for blame performance
func QuickOptimizeRepo(repoPath string, repoName string) error {
	if Verbose {
		fmt.Fprintf(os.Stderr, "Optimizing %s: generating commit-graph...\n", repoName)
	}

	cmd := exec.Command("git", "-C", repoPath, "commit-graph", "write", "--reachable", "--changed-paths")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("commit-graph write failed: %w (output: %s)", err, string(output))
	}

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
			if Verbose {
				fmt.Fprintf(os.Stderr, "Warning: failed to set %s=%s: %v\n", key, value, err)
			}
		}
	}

	if Verbose {
		fmt.Fprintf(os.Stderr, "Optimization complete for %s (commit-graph + config)\n", repoName)
	}

	return nil
}

// FullOptimizeRepo applies deep optimizations including repack
func FullOptimizeRepo(repoPath string, repoName string) error {
	if err := QuickOptimizeRepo(repoPath, repoName); err != nil {
		return err
	}

	if Verbose {
		fmt.Fprintf(os.Stderr, "Optimizing %s: repacking with speed-optimized delta chains...\n", repoName)
	}

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

	cmd = exec.Command("git", "-C", repoPath, "prune-packed")
	if err := cmd.Run(); err != nil {
		if Verbose {
			fmt.Fprintf(os.Stderr, "Warning: prune-packed failed: %v\n", err)
		}
	}

	if Verbose {
		fmt.Fprintf(os.Stderr, "Full optimization complete for %s (commit-graph + config + repack)\n", repoName)
	}

	return nil
}

// ScanDirectory recursively scans a directory for git repositories in parallel
func ScanDirectory(path string, results chan<- RepoInfo, sem chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	sem <- struct{}{}
	defer func() { <-sem }()

	if IsGitRepo(path) {
		results <- RepoInfo{
			Path: path,
			Name: filepath.Base(path),
		}
		return
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			childPath := filepath.Join(path, entry.Name())
			wg.Add(1)
			go ScanDirectory(childPath, results, sem, wg)
		}
	}
}

// FindGitRepos recursively discovers all git repositories under path
func FindGitRepos(path string) ([]RepoInfo, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	if Verbose {
		fmt.Printf("Recursively scanning for git repositories in: %s\n", absPath)
	}
	startTime := time.Now()

	results := make(chan RepoInfo, RepoScanBuffer)

	maxConcurrent := runtime.NumCPU() * ScanConcurrencyMultiplier
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup

	repos := make([]RepoInfo, 0)
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for repo := range results {
			repos = append(repos, repo)
		}
	}()

	wg.Add(1)
	go ScanDirectory(absPath, results, sem, &wg)

	wg.Wait()
	close(results)

	collectorWg.Wait()

	if Verbose {
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

// GetFilesAtHEAD fetches all code files for a repository using gocodewalker
func GetFilesAtHEAD(repoPath string) ([]string, error) {
	fileListQueue := make(chan *gocodewalker.File, FileListQueueBuffer)
	fileWalker := gocodewalker.NewFileWalker(repoPath, fileListQueue)

	fileWalker.AllowListExtensions = GetCodeExtensions()

	fileWalker.IgnoreIgnoreFile = false
	fileWalker.IgnoreGitIgnore = false

	errorHandler := func(e error) bool {
		return true
	}
	fileWalker.SetErrorHandler(errorHandler)

	go func() { _ = fileWalker.Start() }()

	files := make([]string, 0, FileListCapacity)
	for f := range fileListQueue {
		files = append(files, f.Location)
	}

	return files, nil
}

// CountFilesInRepo performs pre-flight file counting for a single repository
func CountFilesInRepo(repo RepoInfo) (RepoInfo, error) {
	fileListQueue := make(chan *gocodewalker.File, FileListQueueBuffer)
	fileWalker := gocodewalker.NewFileWalker(repo.Path, fileListQueue)

	fileWalker.AllowListExtensions = GetCodeExtensions()
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

// PreflightCountFiles counts files in all repositories in parallel and sorts them
func PreflightCountFiles(repos []RepoInfo, fileLister FileLister) []RepoInfo {
	if Verbose {
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

	maxConcurrent := runtime.NumCPU() * PreflightConcurrencyMultiplier
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

			needsOptimize := ShouldOptimizeRepo(r.Path)

			results <- countResult{repo: r, needsOptimize: needsOptimize, err: nil}
		}(repo)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	countedRepos := make([]RepoInfo, 0, len(repos))
	reposNeedingOptimization := 0
	skippedRepos := 0
	for result := range results {
		if result.err != nil {
			fmt.Fprintf(os.Stderr, "Warning: skipping repo %s: %v\n", result.repo.Name, result.err)
			skippedRepos++
			continue
		}
		countedRepos = append(countedRepos, result.repo)
		if result.needsOptimize {
			reposNeedingOptimization++
		}
	}

	sort.Slice(countedRepos, func(i, j int) bool {
		return countedRepos[i].FileCount < countedRepos[j].FileCount
	})

	if skippedRepos > 0 {
		fmt.Fprintf(os.Stderr, "Pre-flight: skipped %d repositories due to errors\n", skippedRepos)
	}

	if Verbose {
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
