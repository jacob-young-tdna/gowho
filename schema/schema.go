package schema

import "fmt"

const (
	DBFile             = "git-who.db"
	BatchSizeThreshold = 5000

	PageSize          = 32768
	CacheSize         = -256000
	MmapSize          = 30000000000
	JournalSizeLimit  = 67108864
	BusyTimeout       = 5000
	WalAutocheckpoint = 1000
)

const SchemaSQL = `
CREATE TABLE file_author (
    repo_path TEXT NOT NULL,
    file_path TEXT NOT NULL,
    author TEXT NOT NULL,
    lines INTEGER DEFAULT 0,
    PRIMARY KEY (repo_path, file_path, author)
) WITHOUT ROWID;

CREATE TABLE author_commit_stats (
    repo_path TEXT NOT NULL,
    author TEXT NOT NULL,
    commits_3m INTEGER DEFAULT 0,
    commits_6m INTEGER DEFAULT 0,
    commits_12m INTEGER DEFAULT 0,
    merges INTEGER DEFAULT 0,
    PRIMARY KEY (repo_path, author)
) WITHOUT ROWID;

CREATE INDEX idx_repo_path ON file_author(repo_path);
CREATE INDEX idx_file_path ON file_author(file_path);
CREATE INDEX idx_author ON file_author(author);
CREATE INDEX idx_lines ON file_author(lines DESC);
`

func Pragmas() []string {
	return []string{
		fmt.Sprintf("PRAGMA page_size=%d", PageSize),
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=OFF",
		fmt.Sprintf("PRAGMA cache_size=%d", CacheSize),
		fmt.Sprintf("PRAGMA mmap_size=%d", MmapSize),
		"PRAGMA temp_store=MEMORY",
		fmt.Sprintf("PRAGMA journal_size_limit=%d", JournalSizeLimit),
		fmt.Sprintf("PRAGMA busy_timeout=%d", BusyTimeout),
		"PRAGMA locking_mode=EXCLUSIVE",
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", WalAutocheckpoint),
		"PRAGMA auto_vacuum=NONE",
	}
}

const QueryTotalLines = "SELECT SUM(lines) FROM file_author"
const QueryTotalFiles = "SELECT COUNT(DISTINCT repo_path, file_path) FROM file_author"
const QueryTotalAuthors = "SELECT COUNT(DISTINCT author) FROM file_author"
const QueryTotalRepos = "SELECT COUNT(DISTINCT repo_path) FROM file_author"

const QueryTopContributors = `
SELECT
    fa.author,
    SUM(fa.lines) as total_lines,
    COALESCE(SUM(acs.commits_3m), 0) as commits_3m,
    COALESCE(SUM(acs.commits_6m), 0) as commits_6m,
    COALESCE(SUM(acs.commits_12m), 0) as commits_12m,
    COALESCE(SUM(acs.merges), 0) as merges
FROM file_author fa
LEFT JOIN author_commit_stats acs ON fa.repo_path = acs.repo_path AND fa.author = acs.author
GROUP BY fa.author
ORDER BY total_lines DESC
LIMIT 10
`
