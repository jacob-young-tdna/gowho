package core

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"gowho/schema"

	_ "modernc.org/sqlite"
)

// CreateDatabase creates the SQLite database with optimized schema
func CreateDatabase() (*sql.DB, error) {
	if Verbose {
		fmt.Printf("Creating database: %s\n", schema.DBFile)
	}

	_ = os.Remove(schema.DBFile)

	db, err := sql.Open("sqlite", schema.DBFile)
	if err != nil {
		return nil, err
	}

	for _, pragma := range schema.Pragmas() {
		if _, err := db.Exec(pragma); err != nil {
			return nil, fmt.Errorf("pragma failed (%s): %w", pragma, err)
		}
		if Verbose {
			fmt.Printf("  Applied: %s\n", pragma)
		}
	}

	_, err = db.Exec(schema.SchemaSQL)
	return db, err
}

// CentralizedDatabaseWriter is the single goroutine responsible for all SQLite writes
func CentralizedDatabaseWriter(db *sql.DB, fileStats <-chan FileAuthorStats,
	commitStats <-chan CommitStatsBatch, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(BatchFlushInterval)
	defer ticker.Stop()

	fileStatsBatch := make([]FileAuthorStats, 0, schema.BatchSizeThreshold)

	flushFileStats := func() {
		if len(fileStatsBatch) == 0 {
			return
		}
		if err := WriteBatch(db, fileStatsBatch); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file stats batch: %v\n", err)
			GlobalMetrics.Errors.Add(1)
		}
		fileStatsBatch = fileStatsBatch[:0]
	}

	fileStatsClosed := false
	commitStatsClosed := false

	for {
		select {
		case stat, ok := <-fileStats:
			if !ok {
				fileStatsClosed = true
				fileStats = nil
				continue
			}
			fileStatsBatch = append(fileStatsBatch, stat)
			if len(fileStatsBatch) >= schema.BatchSizeThreshold {
				flushFileStats()
			}

		case batch, ok := <-commitStats:
			if !ok {
				commitStatsClosed = true
				commitStats = nil
				continue
			}
			repoPath := RepoInterns.Lookup(batch.RepoID)
			if err := WriteCommitStatsBatch(db, repoPath, batch.Stats); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing commit stats: %v\n", err)
				GlobalMetrics.Errors.Add(1)
			}

		case <-ticker.C:
			flushFileStats()
		}

		if fileStatsClosed && commitStatsClosed {
			flushFileStats()
			return
		}
	}
}

// WriteCommitStatsBatch writes commit statistics using optimized multi-row INSERT
func WriteCommitStatsBatch(db *sql.DB, repoPath string, stats map[uint32]*CommitStats) error {
	if len(stats) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var sb strings.Builder
	sb.WriteString("INSERT INTO author_commit_stats VALUES ")

	args := make([]interface{}, 0, len(stats)*6)
	first := true
	for authorID, stat := range stats {
		if !first {
			sb.WriteString(",")
		}
		first = false
		sb.WriteString("(?,?,?,?,?,?)")
		author := AuthorInterns.Lookup(authorID)
		args = append(args, repoPath, author, stat.Commits3M, stat.Commits6M, stat.Commits12M, stat.Merges)
	}

	_, err = tx.Exec(sb.String(), args...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// WriteBatch writes a batch of file author stats using optimized multi-row INSERT
func WriteBatch(db *sql.DB, batch []FileAuthorStats) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var sb strings.Builder
	sb.WriteString("INSERT INTO file_author VALUES ")

	args := make([]interface{}, 0, len(batch)*4)
	for i := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?)")

		repoPath := RepoInterns.Lookup(batch[i].RepoID)
		author := AuthorInterns.Lookup(batch[i].AuthorID)
		args = append(args, repoPath, batch[i].FilePath, author, batch[i].Lines)
	}

	_, err = tx.Exec(sb.String(), args...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GenerateSummary prints final statistics
func GenerateSummary(db *sql.DB) {
	fmt.Printf("Database: %s\n", schema.DBFile)
	fmt.Println()

	var totalLines, totalFiles, totalAuthors, totalRepos int64
	_ = db.QueryRow(schema.QueryTotalLines).Scan(&totalLines)
	_ = db.QueryRow(schema.QueryTotalFiles).Scan(&totalFiles)
	_ = db.QueryRow(schema.QueryTotalAuthors).Scan(&totalAuthors)
	_ = db.QueryRow(schema.QueryTotalRepos).Scan(&totalRepos)

	fmt.Println("Summary Statistics:")
	fmt.Printf("  Total repositories: %d\n", totalRepos)
	fmt.Printf("  Total lines analyzed: %d\n", totalLines)
	fmt.Printf("  Unique files: %d\n", totalFiles)
	fmt.Printf("  Unique authors: %d\n", totalAuthors)
	fmt.Println()

	fmt.Println("Top 10 Contributors (by lines in current HEAD):")
	fmt.Println()

	rows, err := db.Query(schema.QueryTopContributors)
	if err != nil {
		fmt.Printf("Error querying stats: %v\n", err)
		return
	}
	defer func() { _ = rows.Close() }()

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
			Truncate(author, 40), lines, c3m, c6m, c12m, merges)
	}

	fmt.Println()
	fmt.Println("Example queries:")
	fmt.Println()
	fmt.Println("# Lines per author (all repos):")
	fmt.Printf("sqlite3 %s \"SELECT author, SUM(lines) as total FROM file_author GROUP BY author ORDER BY total DESC;\"\n", schema.DBFile)
	fmt.Println()
	fmt.Println("# Lines per author per repo:")
	fmt.Printf("sqlite3 %s \"SELECT repo_path, author, SUM(lines) as total FROM file_author GROUP BY repo_path, author ORDER BY repo_path, total DESC;\"\n", schema.DBFile)
	fmt.Println()
	fmt.Println("# Files per author:")
	fmt.Printf("sqlite3 %s \"SELECT author, COUNT(DISTINCT repo_path, file_path) as files FROM file_author GROUP BY author ORDER BY files DESC;\"\n", schema.DBFile)
	fmt.Println()
	fmt.Println("# Ownership of a specific file:")
	fmt.Printf("sqlite3 %s \"SELECT author, lines, ROUND(lines*100.0/(SELECT SUM(lines) FROM file_author WHERE repo_path='REPO' AND file_path='FILE'),2) as pct FROM file_author WHERE repo_path='REPO' AND file_path='FILE' ORDER BY lines DESC;\"\n", schema.DBFile)
}
