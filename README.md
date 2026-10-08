# 分布式 SQL 数据库 (Capstone 5.2-B)

3 节点 Raft 复制的分布式 SQL 数据库：SQL 子集 + Raft 共识 + B+ 树存储引擎。

## 架构

```
SQL 文本
  ↓
分词器/递归下降解析器 → AST → plan (可序列化的 dict)
  ↓ 写操作                    ↓ 读操作
Raft 日志复制 (3 节点)        Leader 本地读 / Follower 转发给 Leader
  ↓ 提交后 apply
本地存储引擎 (B+ 树 + WAL)
```

**一句话**：SQL 语句在 leader 上解析成 plan dict，经 Raft 复制到多数派后，各节点在本地存储引擎上确定性重放；读走 leader（follower 转发）。

## 快速开始

```bash
# 启动 3 节点集群 (端口 17001-17003)
bash bin/cluster.sh start fresh

# 建表 / 写 / 查
python3 bin/dbsql.py exec "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)"
python3 bin/dbsql.py exec "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25)"
python3 bin/dbsql.py query "SELECT * FROM users WHERE age > 20"

# 交互式 shell
python3 bin/dbsql.py shell

# 停止
bash bin/cluster.sh stop
```

## 跑测试

```bash
bash run_all.sh
# === 1/2 SQL 正确性: 72 个断言
# === 2/2 分布式正确性: 20 个断言 (kill -9 leader / 网络分区 / 重启追赶)
# ALL GREEN ✅
```

## 故障恢复演示

```bash
bash demo/fault_recovery.sh 2>&1 | tee demo/transcript.txt
# 写 1000 行 → kill -9 两台 (含 leader) → 重启 → 三节点 1000 行零丢失 ✅
```

完整 transcript 见 `demo/transcript.txt`。

## SQL 子集

| 语句 | 支持 |
|------|------|
| `CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)` | ✅ (列级或表级主键) |
| `INSERT INTO t VALUES (...), (...)` | ✅ (多行, 列清单可选) |
| `SELECT * / col FROM t WHERE ...` | ✅ (`= < > <= >= <> !=`, `AND/OR/NOT`, 括号, `IS NULL`) |
| `UPDATE t SET ... WHERE ...` | ✅ |
| `DELETE FROM t WHERE ...` | ✅ |
| `DROP TABLE t` | ✅ |

**做不到的** (诚实标注)：`JOIN`、聚合 (`COUNT/SUM/...`)、`ORDER BY`、二级索引、跨表事务。
B+ 树只建在主键上，非主键条件的查询是全表扫描 —— 数据量大时慢，这是设计取舍，不是 bug。

## 设计决策

### 1. 为什么复制 SQL plan 而不是 KV 效果？

研究员备忘录建议复制 KV 效果 (operation-based)，理由是语句复制要求确定性重放。
本项目**实测后决定保留语句复制**，理由：

- SQL 子集是**完全确定性**的：没有 `NOW()`、`RAND()`、UDF。plan dict 是
  `(SQL 文本 + catalog)` 的纯函数，所有节点按相同日志顺序重放，结果必然一致。
- Raft 日志**人类可读**：`/debug` 能直接看到 SQL plan，教学和演示价值高。
- 分布式测试证明：kill -9 / 分区 / 重启场景下三节点行集逐行一致。

诚实标注：如果将来加入非确定函数，必须切换到效果复制或禁用这些函数。

### 2. 读路径：leader 读

- Leader 直接读本地已提交数据 (leaseholder 读)。
- Follower 把 SELECT 转发给 leader；若 leader 未知或转发失败，降级为本地读
  (返回里明确标注 `stale_read_possible: true`)。
- 不做 quorum 读：单一致性级别先把正确性做对。

### 3. 写路径：单语句原子

每条写语句 = 一条 Raft 日志 = 一次本地 MiniDB 事务提交。`applied` 游标
(已应用的最大 Raft index) 和语句效果**在同一个 MiniDB 事务里提交** ——
这是 "Raft 提交 + WAL 双保险" 咬合的关键 (详见 `docs/blogs/03-故障恢复.md`)。

多语句事务 (`BEGIN/COMMIT`) 明确不支持，见"局限"。

### 4. Schema 即数据

表结构作为一条特殊的 KV (`("\x00catalog", 表名)`) 存在同一个 B+ 树里，
随 Raft 日志复制。`CREATE/DROP TABLE` 是单条原子日志，不需要备忘录说的
"双写过渡" —— 那是给 `ALTER TABLE` 这种在线改列准备的，本项目没有 `ALTER`。

## 诚实标注的局限

1. **无 JOIN/聚合/二级索引**：全表扫描，数据量大时慢。
2. **无多语句事务**：只有单语句原子性。`BEGIN` 会报语法错误。
3. **Raft 日志无压缩**：长期运行日志无限增长 (内存 + 磁盘)。生产环境需要 snapshot。
4. **Follower 降级读可能 stale**：已明确标注，调用方需知晓。
5. **脑裂时少数派拒绝写**：这是 Raft 的正确行为，不是 bug。客户端会收到
   `not leader` 并跟随 leader hint 重试。
6. **运维红线**：同一数据目录只允许一个节点进程 (有 `flock` 互斥)。
   **禁止 `rm -rf` 数据目录时有节点在运行** —— 会换掉 inode，锁不住，
   曾实测导致 B+ 树页号复用与成环 (已修复，见下)。
7. **单机性能**：纯 Python 实现，B+ 树 order=4，buffer pool 64 页。
   1000 行写入约 30 秒。教学项目，不追求性能。

## 实测中抓到的真 bug (已修复)

开发过程中实测抓到三个真 bug，都已修复并有回归测试覆盖：

1. **页号复用 → B+ 树成环 → 持锁线程卡死**：
   `allocate_page()` 只做 buffered write 不 sync，kill -9 可能丢掉文件扩展；
   重启后 `num_pages()` 变小，新页号复用已存在的页。
   修复：内存高水位 `_next_pid`，启动时用 WAL 最大页号初始化，单调递增永不复用。

2. **`_apply_new` 先推进 `last_applied` 再 apply**：
   若 apply 抛异常，该条目被永久跳过。修复：成功后才推进。

3. **恢复时 Redo 顺序错误**：
   按 LSN 混排重放时，早期的 COMMIT 会在后期的 PAGE_WRITE (新 root 页)
   还没重放时，用 final root 查全零新页。修复：Redo 分两遍，
   先物理 (PAGE_WRITE) 再逻辑 (KV_UPDATE/COMMIT)。

另加纵深防御：B+ 树 `_find_leaf`/`scan` 成环检测 (抛错而非卡死)，
`TableStore` 的 `flock` 互斥。

详见 `VENDOR.md`。

## 复用 vs 新写

| 部分 | 来源 | 状态 |
|------|------|------|
| Raft (`src/vendor/raft/`) | distributed-systems skill | 1 处修改 (`_apply_new`) |
| 存储引擎 (`src/vendor/minidb/`) | db-engine skill | 3 处修改 (页号高水位/两遍 Redo/成环检测) |
| SQL 层 (`src/sql/`) | **新写** | 740 行 |
| 存储适配 (`src/storage/`) | **新写** | 147 行 |
| 分布式节点 (`src/node/`) | **新写** | 212 行 |
| 客户端/脚本/测试/演示 | **新写** | 787 行 |

新写代码 **1886 行** (≥1500 目标)，详见 `VENDOR.md`。

## CI

`.github/workflows/ci.yml` 已配置 (GitHub Actions, Python 3.12, `bash run_all.sh`)。
**CI 配置已写，待 push 后验证。**

## 博客草稿

`docs/blogs/` 下 3 篇草稿 (只写草稿，不发布)：

1. `01-为什么选SQL-on-Raft.md` —— 设计决策与 trade-off
2. `02-Raft日志条目设计.md` —— 写操作如何序列化与 apply
3. `03-故障恢复.md` —— Raft 提交 + WAL 双保险是怎么咬合的
