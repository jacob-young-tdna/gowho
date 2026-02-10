#!/bin/bash
#
# This tool can be used to compared the git-who sqlite DB v.s. the gowho sqlite DB

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <db1.db> <db2.db>"
    exit 1
fi

DB1="$1"
DB2="$2"

if [ ! -f "$DB1" ]; then
    echo "Error: Database file '$DB1' not found"
    exit 1
fi

if [ ! -f "$DB2" ]; then
    echo "Error: Database file '$DB2' not found"
    exit 1
fi

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

run_query() {
    local db=$1
    local query=$2
    local output=$3
    sqlite3 -csv -header "$db" "$query" > "$output"
}

compare_results() {
    local query_name=$1
    local file1=$2
    local file2=$3

    echo "=========================================="
    echo "Query: $query_name"
    echo "=========================================="

    if diff -u "$file1" "$file2" > "$TEMP_DIR/diff_output" 2>&1; then
        echo "✓ Results match"
    else
        echo "✗ Results differ:"
        echo ""
        cat "$TEMP_DIR/diff_output"
        echo ""
    fi
    echo ""
}

# Query 1: Lines per author (all repos)
QUERY1="SELECT author, SUM(lines) as total FROM file_author GROUP BY author ORDER BY total DESC;"
run_query "$DB1" "$QUERY1" "$TEMP_DIR/db1_q1.csv"
run_query "$DB2" "$QUERY1" "$TEMP_DIR/db2_q1.csv"
compare_results "Lines per author (all repos)" "$TEMP_DIR/db1_q1.csv" "$TEMP_DIR/db2_q1.csv"

# Query 2: Lines per author per repo
QUERY2="SELECT repo_path, author, SUM(lines) as total FROM file_author GROUP BY repo_path, author ORDER BY repo_path, total DESC;"
run_query "$DB1" "$QUERY2" "$TEMP_DIR/db1_q2.csv"
run_query "$DB2" "$QUERY2" "$TEMP_DIR/db2_q2.csv"
compare_results "Lines per author per repo" "$TEMP_DIR/db1_q2.csv" "$TEMP_DIR/db2_q2.csv"

# Query 3: Files per author
QUERY3="SELECT author, COUNT(DISTINCT repo_path || '|' || file_path) as files FROM file_author GROUP BY author ORDER BY files DESC;"
run_query "$DB1" "$QUERY3" "$TEMP_DIR/db1_q3.csv"
run_query "$DB2" "$QUERY3" "$TEMP_DIR/db2_q3.csv"
compare_results "Files per author" "$TEMP_DIR/db1_q3.csv" "$TEMP_DIR/db2_q3.csv"

echo "Comparison complete."
