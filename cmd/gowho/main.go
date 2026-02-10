package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"gowho/core"
)

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
	command := "analyze"
	argOffset := 1
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		if os.Args[1] == "optimize" || os.Args[1] == "analyze" {
			command = os.Args[1]
			argOffset = 2
		}
	}

	if command == "optimize" {
		targetPath := "."
		optimizationMode := core.OptimizeQuick
		forceOptimize := false
		jobs := runtime.NumCPU()

		for i := argOffset; i < len(os.Args); i++ {
			arg := os.Args[i]
			switch {
			case arg == "-h" || arg == "--help":
				printHelp()
				os.Exit(0)
			case arg == "-v" || arg == "--verbose":
				core.Verbose = true
			case strings.HasPrefix(arg, "--mode="):
				v := strings.TrimPrefix(arg, "--mode=")
				switch v {
				case "quick":
					optimizationMode = core.OptimizeQuick
				case "full":
					optimizationMode = core.OptimizeFull
				default:
					fmt.Fprintf(os.Stderr, "Unknown --mode=%s (use quick|full)\n", v)
					os.Exit(2)
				}
			case arg == "-m" && i+1 < len(os.Args):
				i++
				v := os.Args[i]
				switch v {
				case "quick":
					optimizationMode = core.OptimizeQuick
				case "full":
					optimizationMode = core.OptimizeFull
				default:
					fmt.Fprintf(os.Stderr, "Unknown -m %s (use quick|full)\n", v)
					os.Exit(2)
				}
			case strings.HasPrefix(arg, "--jobs="):
				v := strings.TrimPrefix(arg, "--jobs=")
				var parsed int
				_, err := fmt.Sscanf(v, "%d", &parsed)
				if err != nil || parsed < 1 {
					fmt.Fprintf(os.Stderr, "Invalid --jobs=%s (use positive integer)\n", v)
					os.Exit(2)
				}
				jobs = parsed
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

		core.RunOptimizeCommand(targetPath, optimizationMode, forceOptimize, jobs)
		return
	}

	// ANALYZE COMMAND (default)
	targetPath := "."
	fileListMode := core.FileListModeDefault

	for i := argOffset; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch {
		case arg == "-h" || arg == "--help":
			printHelp()
			os.Exit(0)
		case arg == "-v" || arg == "--verbose":
			core.Verbose = true
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

	core.RunAnalyzeCommand(targetPath, fileListMode)
}
