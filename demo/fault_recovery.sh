#!/usr/bin/env bash
# 故障恢复演示: 写 1000 行 -> kill -9 两台 -> 重启 -> 数据全在.
# 用法: bash demo/fault_recovery.sh 2>&1 | tee demo/transcript.txt
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$DIR")"
export PYTHONPATH="$ROOT/src"
DBSQL="python3 $ROOT/bin/dbsql.py"

echo "================================================================"
echo " 分布式 SQL 数据库: 故障恢复演示"
echo " 3 节点 Raft 集群, 写 1000 行 -> kill -9 两台 -> 重启 -> 数据全在"
echo "================================================================"
echo

echo "### 1. 启动全新 3 节点集群"
bash "$ROOT/bin/cluster.sh" start fresh
sleep 4
$DBSQL status
echo

echo "### 2. 建表"
$DBSQL exec "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)"
echo

echo "### 3. 写入 1000 行 (100 条 INSERT, 每条 10 行)"
START=$(date +%s)
python3 - "$ROOT" <<'EOF'
import sys, subprocess
sys.path.insert(0, sys.argv[1] + "/src")
# 用 dbsql 批量写
for batch in range(100):
    base = batch * 10 + 1
    vals = ", ".join(f"({base+i}, 'user{base+i}', {20 + (base+i) % 50})"
                     for i in range(10))
    sql = f"INSERT INTO users VALUES {vals}"
    r = subprocess.run(["python3", sys.argv[1] + "/bin/dbsql.py", "exec", sql],
                       capture_output=True, text=True)
    if "ok" not in r.stdout.lower() and "1 row" not in r.stdout and "10" not in r.stdout:
        # dbsql 输出格式检查: 只要没报错就行
        if "error" in r.stdout.lower():
            print(f"batch {batch} FAILED: {r.stdout[:200]}")
            sys.exit(1)
print("1000 rows written")
EOF
END=$(date +%s)
echo "写入耗时: $((END - START)) 秒"
echo

echo "### 4. 确认三节点各有 1000 行"
for i in 0 1 2; do
  n=$($DBSQL --node $i exec "SELECT id FROM users" 2>/dev/null | grep -c "^| [0-9]" || true)
  echo "node $i: $n 行"
done
echo

LEADER=$($DBSQL status | grep leader | head -1 | sed 's/.*node \([0-9]\).*/\1/')
echo "### 5. kill -9 两台 (含 leader)"
# 找出 leader, 杀 leader + 另一个节点, 留一台活着
L=$($DBSQL exec "SELECT id FROM users WHERE id=1" 2>&1 | head -1 >/dev/null; \
    python3 -c "
import json,urllib.request
for i in range(3):
    try:
        s=json.load(urllib.request.urlopen(f'http://127.0.0.1:1700{i+1}/status',timeout=2))
        if s.get('role')=='leader': print(i); break
    except: pass")
echo "当前 leader = node $L"
OTHERS=$(python3 -c "print([i for i in [0,1,2] if i != $L])")
F1=$(python3 -c "print([i for i in [0,1,2] if i != $L][0])")
echo "kill node $L (leader) 和 node $F1 ..."
bash "$ROOT/bin/cluster.sh" kill "$L"
bash "$ROOT/bin/cluster.sh" kill "$F1"
sleep 2
SURVIVOR=$(python3 -c "print([i for i in [0,1,2] if i not in ($L,$F1)][0])")
echo "存活: node $SURVIVOR (单节点无多数派, 集群暂时不可写 -- 这是 Raft 的设计, 不是 bug)"
echo

echo "### 6. 重启被 kill 的两台, 等它们追上日志"
bash "$ROOT/bin/cluster.sh" restart "$L"
bash "$ROOT/bin/cluster.sh" restart "$F1"
echo "等待选主与日志追赶..."
sleep 8
$DBSQL status
echo

echo "### 7. 验证: 三节点数据全在, 1000 行零丢失"
FAIL=0
for i in 0 1 2; do
  n=$($DBSQL --node $i exec "SELECT id FROM users" 2>/dev/null | grep -c "^| [0-9]" || true)
  # 抽查首尾行内容
  first=$($DBSQL --node $i exec "SELECT id, name, age FROM users WHERE id=1" 2>/dev/null | grep "^| 1 " || true)
  last=$($DBSQL --node $i exec "SELECT id, name, age FROM users WHERE id=1000" 2>/dev/null | grep "^| 1000 " || true)
  if [[ "$n" == "1000" && -n "$first" && -n "$last" ]]; then
    echo "node $i: ✅ 1000 行, 首尾行内容正确"
  else
    echo "node $i: ❌ 行数=$n (期望 1000)"
    FAIL=1
  fi
done
echo
if [[ $FAIL == 0 ]]; then
  echo "================================================================"
  echo " 演示成功: kill -9 两台后重启, 1000 行数据零丢失 ✅"
  echo "================================================================"
else
  echo "演示失败 ❌"
  exit 1
fi
