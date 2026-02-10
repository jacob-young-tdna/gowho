package core

import (
	"fmt"
	"time"
)

// ProgressUI manages the terminal UI for multi-repo progress tracking
func ProgressUI(progressChan <-chan RepoProgress, done chan<- struct{}) {
	defer close(done)

	repoStates := make(map[string]*RepoProgress)
	repoOrder := make([]string, 0)

	completedCount := 0
	erroredCount := 0

	startTime := time.Now()

	ticker := time.NewTicker(UITickerInterval)
	defer ticker.Stop()

	fmt.Print("\033[2J\033[H\033[?25l")
	defer fmt.Print("\033[?25h")

	lastLineCount := 0

	renderUI := func() {
		fmt.Print("\033[H")

		lines := 0

		var queued, active int
		for _, progress := range repoStates {
			switch progress.Status {
			case StatusQueued:
				queued++
			case StatusOptimizing, StatusDiscovering, StatusBlaming, StatusCommits:
				active++
			}
		}

		elapsed := time.Since(startTime)
		hours := int(elapsed.Hours())
		minutes := int(elapsed.Minutes()) % 60
		seconds := int(elapsed.Seconds()) % 60
		runtime := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)

		fmt.Printf("┌─ Git Who  ────────────────────────────────────────────────────────────────────────────────────────────────────┐\n")
		lines++
		fmt.Printf("│ Runtime: %s | Active: %d | Done: %d | Queued: %d | Errors: %d | Skipped: %d | Files: %d | Lines: %d     \n",
			runtime, active, completedCount, queued, erroredCount, GlobalMetrics.FilesSkipped.Load(), GlobalMetrics.FilesProcessed.Load(), GlobalMetrics.LinesProcessed.Load())
		lines++
		fmt.Printf("└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘\n")
		lines++
		fmt.Println()
		lines++

		visibleRepos := selectVisibleRepos(repoStates, repoOrder)

		for _, name := range visibleRepos {
			progress := repoStates[name]
			if progress != nil {
				lines += renderRepoLine(progress)
			}
		}

		hiddenQueued := queued - countVisibleQueued(repoStates, visibleRepos)
		if hiddenQueued > 0 {
			fmt.Printf("\033[90m... and %d more queued repositories\033[0m\n", hiddenQueued)
			lines++
		}

		for i := lines; i < lastLineCount; i++ {
			fmt.Print("\033[K\n")
		}
		lastLineCount = lines
	}

	for {
		select {
		case progress, ok := <-progressChan:
			if !ok {
				renderUI()
				fmt.Println()
				return
			}

			if _, exists := repoStates[progress.Name]; !exists {
				repoOrder = append(repoOrder, progress.Name)
			}

			prevStatus := StatusQueued
			if prev, exists := repoStates[progress.Name]; exists {
				prevStatus = prev.Status
			}

			repoStates[progress.Name] = &progress

			if progress.Status == StatusDone && prevStatus != StatusDone {
				completedCount++
				if completedCount > MaxCompletedTracking {
					delete(repoStates, progress.Name)
				}
			} else if progress.Status == StatusError && prevStatus != StatusError {
				erroredCount++
			}

		case <-ticker.C:
			renderUI()
		}
	}
}

func selectVisibleRepos(repoStates map[string]*RepoProgress, repoOrder []string) []string {
	scoreRepo := func(status RepoStatus) int {
		switch status {
		case StatusOptimizing, StatusDiscovering, StatusBlaming, StatusCommits:
			return 1000
		case StatusError:
			return 900
		case StatusDone:
			return 100
		case StatusQueued:
			return 1
		default:
			return 50
		}
	}

	type scoredRepo struct {
		name  string
		score int
	}

	scored := make([]scoredRepo, 0, len(repoOrder))
	for _, name := range repoOrder {
		progress := repoStates[name]
		if progress == nil {
			continue
		}
		scored = append(scored, scoredRepo{
			name:  name,
			score: scoreRepo(progress.Status),
		})
	}

	visible := make([]string, 0, MaxVisibleRepos)

	for _, sr := range scored {
		if sr.score >= 900 && len(visible) < MaxVisibleRepos {
			visible = append(visible, sr.name)
		}
	}

	for _, sr := range scored {
		if sr.score < 100 && len(visible) < MaxVisibleRepos {
			visible = append(visible, sr.name)
		}
	}

	return visible
}

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

func renderRepoLine(p *RepoProgress) int {
	fmt.Print("\033[K")

	displayName := p.Name
	if len(displayName) > MaxRepoNameLength {
		displayName = displayName[:MaxRepoNameLength-3] + "..."
	}

	var statusIcon, statusText, color string
	switch p.Status {
	case StatusQueued:
		statusIcon = StatusIconQueued
		statusText = "Queued"
		color = "\033[90m"
	case StatusOptimizing:
		statusIcon = StatusIconOptimizing
		statusText = "Optimizing repository"
		color = "\033[95m"
	case StatusDiscovering:
		statusIcon = StatusIconDiscovering
		statusText = "Discovering files"
		color = "\033[36m"
	case StatusBlaming:
		statusIcon = StatusIconBlaming
		if p.FilesSkipped > 0 {
			statusText = fmt.Sprintf("Blaming %d/%d files (%d skipped)", p.FilesBlamed, p.FilesTotal, p.FilesSkipped)
		} else {
			statusText = fmt.Sprintf("Blaming %d/%d files", p.FilesBlamed, p.FilesTotal)
		}
		color = "\033[33m"
	case StatusCommits:
		statusIcon = StatusIconCommits
		statusText = "Analyzing commits"
		color = "\033[35m"
	case StatusDone:
		statusIcon = StatusIconDone
		if p.FilesSkipped > 0 {
			statusText = fmt.Sprintf("Complete (%d files, %d lines, %d skipped)", p.FilesTotal, p.LinesBlamed, p.FilesSkipped)
		} else {
			statusText = fmt.Sprintf("Complete (%d files, %d lines)", p.FilesTotal, p.LinesBlamed)
		}
		color = "\033[32m"
	case StatusError:
		statusIcon = StatusIconError
		statusText = "Error: " + p.ErrorMessage
		color = "\033[31m"
	default:
		statusIcon = "[?]"
		statusText = p.Status.String()
		color = "\033[0m"
	}

	progressBar := ""
	if p.Status == StatusBlaming && p.FilesTotal > 0 {
		filled := int(float64(p.FilesBlamed) / float64(p.FilesTotal) * float64(ProgressBarWidth))
		if filled > ProgressBarWidth {
			filled = ProgressBarWidth
		}
		progressBar = " ["
		for i := 0; i < ProgressBarWidth; i++ {
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
		color, statusIcon, MaxRepoNameLength, displayName, statusText, progressBar)

	return 1
}
