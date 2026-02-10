# gowho

Tech DNA analysis requires running `git blame` on every file in every repository in a source drop.

gowho parallelizes lame across repositories and files simultaneously, streaming the output directly into a SQLite database.

## What it does

- Recursively discovers all git repositories under a target directory.
- Runs `git blame --line-porcelain` on every tracked file at HEAD across all discovered repos.
- Collects commit frequency statistics (3-month, 6-month, 12-month windows) and merge counts per author per repo.
- Writes all results to `git-who.db`, a SQLite database with two tables
  - `file_author` (line counts per author per file)
  - `author_commit_stats` (commit activity buckets).
- Displays a real-time TUI showing per-repo progress, ETA, active workers, and error counts.

## It's fast...

gowho uses a tiered concurrency model built on goroutines and channels:

- **Repo workers** process multiple repositories in parallel (up to 4 concurrent repos).
- **Blame workers** run `2x CPU count` parallel blame processes per repo, streaming output through reusable 256KB buffers.
- **Centralized writer** batches database inserts (5000 records per flush) through a single writer goroutine to avoid SQLite lock contention.
- **String interning** compresses author emails and repo paths into integer IDs, cutting per-record memory from 40 bytes to 28 bytes.
- **10-second timeout** per file skips pathological blames (binary files, massive generated code) instead of blocking the pipeline.
- **Git optimization pass** (`gowho optimize`) repacks repositories with bitmap indexes and commit-graphs for 3-7x faster blame on repos with deep history.

SQLite is tuned aggressively: WAL mode, 256MB page cache, memory-mapped I/O, exclusive locking, synchronous off. The database is disposable and rebuilt on each run.

## Install and run

```bash
# Install Go (macOS)
brew install go

# Clone and build
git clone <repo-url> && cd gowho
go build -o gowho main.go

# Optional: optimize repos first for large source drops (one-time cost)
./gowho optimize /path/to/source-drop

# Run analysis
./gowho analyze /path/to/source-drop

# Query results
sqlite3 git-who.db "SELECT author, SUM(lines) as total FROM file_author GROUP BY author ORDER BY total DESC;"
```

## Usage

```
gowho [COMMAND] [OPTIONS] [PATH]

COMMANDS:
  analyze       Analyze repositories (default if omitted)
  optimize      Optimize repositories for faster analysis

ANALYZE OPTIONS:
  -v, --verbose           Enable verbose output
  -L, --file-list=MODE    File listing mode: git (default) or fs
                            git: uses git ls-tree (tracked files only, faster)
                            fs:  filesystem walk (respects .gitignore)

OPTIMIZE OPTIONS:
  -v, --verbose           Enable verbose output
  -m, --mode=MODE         quick (default, 3-3.5x speedup) or full (5-7x speedup)
  -j, --jobs=N            Parallel optimization workers (default: all CPUs)
      --force             Re-optimize even if already done
```

## Output schema

The `git-who.db` database contains two tables:

```sql
-- Line ownership at HEAD
CREATE TABLE file_author (
    repo_path TEXT,
    file_path TEXT,
    author    TEXT,
    lines     INTEGER,
    PRIMARY KEY (repo_path, file_path, author)
);

-- Commit activity per author per repo
CREATE TABLE author_commit_stats (
    repo_path    TEXT,
    author       TEXT,
    commits_3m   INTEGER,  -- commits in last 3 months
    commits_6m   INTEGER,  -- commits in last 6 months
    commits_12m  INTEGER,  -- commits in last 12 months
    merges       INTEGER,
    PRIMARY KEY (repo_path, author)
);
```

## Example queries

```sql
-- Top authors by total lines across all repos
SELECT author, SUM(lines) as total
FROM file_author GROUP BY author ORDER BY total DESC;

-- Lines per author in a specific repo
SELECT author, SUM(lines) as total
FROM file_author WHERE repo_path = 'myrepo' GROUP BY author ORDER BY total DESC;

-- Most active contributors in the last 3 months
SELECT author, SUM(commits_3m) as recent
FROM author_commit_stats GROUP BY author ORDER BY recent DESC;

-- File ownership percentage
SELECT author, lines,
  ROUND(lines * 100.0 / (SELECT SUM(lines) FROM file_author
    WHERE repo_path = 'REPO' AND file_path = 'FILE'), 2) as pct
FROM file_author WHERE repo_path = 'REPO' AND file_path = 'FILE'
ORDER BY lines DESC;
```

## Requirements

- Go 1.23+
- Git installed and on PATH
- Only direct dependencies:
  - `modernc.org/sqlite` (pure-Go SQLite, no CGO) allows for cross-compiling to multiple architectures including Windows
  - `github.com/boyter/gocodewalker` (gitignore-aware filesystem walker) used internally by [SCC](https://github.com/boyter/scc)
