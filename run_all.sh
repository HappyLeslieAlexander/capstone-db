#!/usr/bin/env bash
# 一键全绿: SQL 正确性 + 分布式正确性.
# 用法: bash run_all.sh
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="$DIR/src"

echo "=== 1/2 SQL 正确性 (tests/test_sql.py) ==="
python3 "$DIR/tests/test_sql.py" || exit 1
echo
echo "=== 2/2 分布式正确性 (tests/test_distributed.py) ==="
python3 "$DIR/tests/test_distributed.py" || exit 1
echo
echo "ALL GREEN ✅"
