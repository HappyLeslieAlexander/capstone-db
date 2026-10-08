#!/usr/bin/env bash
# 3 节点分布式 SQL 集群 (端口 17001-17003).
# usage: cluster.sh start [fresh] | stop | status | restart <id>
#
# 运维红线 (2026-10-09 血泪教训):
# - 任何时候同一数据目录只允许一个 db_node 进程 (TableStore 有 flock 互斥,
#   但 rm -rf 数据目录是禁止操作: 会换掉 inode, 锁不住).
# - start 前必须确认旧进程已死 (本脚本自动等).
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$DIR")"
VAR="$ROOT/var"
mkdir -p "$VAR/logs" "$VAR/data"

# 等指定模式的进程全部消失 (最多 10 秒)
wait_dead() {
  local pat="$1" i
  for i in $(seq 1 100); do
    if ! pgrep -f "$pat" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "ERROR: 进程 $pat 10 秒还没死, 拒绝继续 (先手动处理)" >&2
  return 1
}

kill_node() {
  local id="$1"
  if [[ -f "$VAR/pid-$id" ]]; then
    local pid
    pid="$(cat "$VAR/pid-$id")"
    # 只杀确实是我们节点的进程 (防 pid 复用误杀)
    if ps -p "$pid" -o args= 2>/dev/null | grep -q "db_node.py $id "; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$VAR/pid-$id"
  fi
  # 兜底: 按命令行精确匹配
  pkill -9 -f "src/node/db_node.py $id --data-dir" 2>/dev/null || true
}

case "${1:-}" in
  start)
    # 先确保没有旧节点活着, 再碰数据目录 (防 rm -rf 与运行中进程竞态)
    for i in 0 1 2; do kill_node "$i"; done
    wait_dead "src/node/db_node.py" || exit 1
    if [[ "${2:-}" == "fresh" ]]; then
      rm -rf "$VAR"/data/*
    fi
    for p in 17001 17002 17003; do
      if (ss -tln 2>/dev/null || netstat -tln 2>/dev/null) | grep -q ":$p "; then
        echo "port $p already in use, aborting"; exit 1
      fi
    done
    for i in 0 1 2; do
      python3 "$ROOT/src/node/db_node.py" "$i" --data-dir "$VAR/data" \
        >"$VAR/logs/node-$i.log" 2>&1 &
      echo $! >"$VAR/pid-$i"
    done
    echo "started 3 db-nodes, pids: $(cat $VAR/pid-*)"
    ;;
  stop)
    for i in 0 1 2; do kill_node "$i"; done
    wait_dead "src/node/db_node.py" || exit 1
    echo "stopped"
    ;;
  kill)
    # 只杀不停 (供故障注入用); restart 会先 kill 再起
    id="${2:?usage: cluster.sh kill <id>}"
    kill_node "$id"
    wait_dead "src/node/db_node.py $id " || exit 1
    echo "killed node $id"
    ;;
  restart)
    id="${2:?usage: cluster.sh restart <id>}"
    kill_node "$id"
    wait_dead "src/node/db_node.py $id " || exit 1
    sleep 0.3
    python3 "$ROOT/src/node/db_node.py" "$id" --data-dir "$VAR/data" \
      >"$VAR/logs/node-$id.log" 2>&1 &
    echo $! >"$VAR/pid-$id"
    echo "restarted node $id (pid $(cat $VAR/pid-$id))"
    ;;
  status)
    for i in 0 1 2; do
      s=$(curl -s -m 2 "http://127.0.0.1:1700$((i+1))/status" 2>/dev/null || echo '{}')
      echo "node $i: $s"
    done
    ;;
  *)
    echo "usage: cluster.sh start [fresh] | stop | status | restart <id>"; exit 1
    ;;
esac
