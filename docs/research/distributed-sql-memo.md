# 分布式 SQL 架在复制 KV 之上的研究备忘录（5.2-B 用）

> 研究员：深挖 subagent · 日期：2026-10-09
> 视角：生产级分布式 SQL 数据库（CockroachDB、TiDB、Calvin）如何把 SQL 架在复制 KV 之上，提炼对教学级 3 节点 Python 系统直接可用的设计建议。
> 约定：每个关键断言附来源链接；不确定的标注"未验证"。

---

## 1. 写路径：CockroachDB 如何把 SQL 写映射成 KV 操作再进 Raft

CockroachDB 是经典的分层架构：SQL 层 → 事务层 → 分布式 KV 层 → 复制层（Raft）→ 存储层（Pebble LSM）。SQL 层把语句解析、优化、执行成"KV 操作计划"，一行 SQL 记录被编码成一组 KV 条目：key 编码表 ID、索引 ID、主键列值（字节可比编码、保序），value 编码其余列；二级索引另有自己的 KV 条目，key = 索引列值 + 主键，实现 covering 索引扫描免回表([letsbuildsolutions](https://letsbuildsolutions.com/blog/system-design/how-cockroachdb-works-internally-distributed-sql-raft-consensus-per-range-and-the-architecture-behind-serializable-transactions-at-global-scale/))。

跨 range 的写走分布式事务协议：先在参与最重的 range 写一条 PENDING 状态的事务记录，同时在各 range 的 key 上写 MVCC **写意图（write intent）**——即带特殊标记的未提交版本（含 txn id 用于冲突裁决）；提交时事务记录改为 COMMITTED，意图随后异步解析为正式版本([cockroach docs/design.md](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md))。每个 range 是一个独立 Raft 组，写操作在 range 的 Raft 组内复制达成共识后才向客户端返回写时间戳([continuum 架构](https://www.cockroachlabs.com/blog/continuum-architecture/))。为减少两阶段提交延迟，CockroachDB 用 **Parallel Commits**：事务协调者把事务记录写成 STAGING 状态并把所有 intent 地址记录下来，客户端即可认为提交成功返回——"意向已全部落地 + 提交记录可恢复 = 已提交"，冲突事务可代为推进提交([Medium: serializable transactions](https://medium.com/@ariyanwaliya/inside-cockroachdb-the-engineering-behind-serializable-transactions-8d2946e2de2c))。冲突处理走乐观路线：遇到冲突把对方事务时间戳 push 或让其重试，保证串行化([docs/design.md](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md))。

**Statement-based vs operation-based 复制**：CockroachDB 走的是"复制 KV 效果"（近似 row-based/operation-based）——Raft 日志里装的是 KV 层面的写操作（intent、事务记录、split 等），而非 SQL 语句。MySQL 生态对两者的比较说明 trade-off：statement-based 日志小但要求语句确定性（`NOW()`、随机数、UDF 在主从重放结果不一致即分叉）；row-based 记录每一行的变更，更安全、代价是日志量大([Database Journal](https://www.databasejournal.com/mysql/comparing-mysql-statement-based-and-row-based-replication/))。对 Raft 复制而言还要加上：statement 复制要求所有副本**确定性重放**——Calvin 论文明确警告非确定逻辑（读时钟、随机数、文件/网络 I/O）会导致副本状态分叉([ankurah 文献综述](https://github.com/ankurah/ankurah/blob/HEAD/specs/concurrency/lit-review/07-database-internals.md))。因此生产系统普遍选 operation/effect 复制：牺牲日志带宽，换取正确性简单、易验证。

---

## 2. 读路径：leaseholder 读、linearizable 读、follower 读

CockroachDB 对读提供三档一致性选择（官方默认 SSI 串行化隔离，锁无关读）([docs/design.md](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md))：

1. **leaseholder 读（默认强一致读）**：每个 range 选一个持有"租约"的副本作为读写协调者，读直接找 leaseholder 本地服务，无需 Raft 往返——通过租约保证它仍是有效主，避免 quorum 读的高延迟；负载持续时租约会迁移到靠近客户端的节点（follow-the-workload）([HN 讨论](https://news.ycombinator.com/item?id=20472640))。代价是租约管理复杂度，但读取延迟≈单次本地读。
2. **linearizable 读**：严格的线性一致读，保证读到已提交的最新写（leaseholder 读本身在 CockroachDB 中即提供线性化语义，未显式选 stale 即可得）。其代价是读必须经过当前 leaseholder/最新提交点，不能读本地过期副本；这在"正确性优先于延迟"的默认立场下被接受([docs/design.md](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md))。
3. **follower 读（stale）**：`AS OF SYSTEM TIME` 历史读，可指定精确 staleness 或 bounded staleness，读副本本地的旧快照。代价是读到的是过去某一时刻的数据，但对 follower 节点读不打扰写事务的 intent，大幅减少冲突与 `RETRY_SERIALIZABLE` 错误；只有读时间戳低于 closed timestamp 时才不可被写失效([官方性能最佳实践](https://www.cockroachlabs.com/docs/dev/performance-best-practices-overview.html))。

三档的取舍本质是 CAP/PACELC 的一致性—延迟换档：leaseholder 读是默认最优折中，follower 读是为读密集/分析型负载显式降级的逃生门。教学系统若只有一个一致性级别，实现"leaseholder 读"即默认单主读是最务实的选择。

---

## 3. Schema 变更：CockroachDB 的 schema changer 思想

CockroachDB 的 DDL 全部在线、无停机：schema 变更异步复制到所有节点，但变更期间新老 schema 必须共存。老方案（legacy schema changer）是**指令式**的：把 DDL 翻译成硬编码的状态机步骤，descriptor 的 mutations 字段按 `DELETE_ONLY → WRITE_ONLY → BACKFILL_ONLY → PUBLIC` 等状态逐步推进——每个状态严格定义读写双方（旧代码/新代码）对该元素可见到什么，例如加列时先让所有节点"写新格式但仍读旧格式"，回填完再公开([cockroachdb/schemachanger CLAUDE.md](https://github.com/cockroachdb/cockroach/blob/HEAD/./pkg/sql/schemachanger/CLAUDE.md))。

新方案（declarative schema changer）把变更建模为**元素+目标状态**的声明式规划：每个元素（列、索引、约束）有目标状态（PUBLIC/ABSENT/TRANSIENT_ABSENT），用规则和依赖图自动生成执行计划，再由作业系统（`NEW_SCHEMA_CHANGE` job）在后台推进([同上](https://github.com/cockroachdb/cockroach/blob/HEAD/./pkg/sql/schemachanger/CLAUDE.md))。核心思想可提炼为三句：① **schema 本身是带版本的元数据**，存在 KV 里，和数据一样被复制；② **变更分阶段、双写过渡**，保证混合版本节点互不踩踏；③ **长操作（建索引回填）异步化**为后台 job，DDL 语句立即返回。注意一个实操坑：变更未在所有节点完成前不能对同一表再做变更，大规模部署需串行等待([Liquibase 实战笔记](https://dev.to/bobferris/using-liquibase-with-cockroachdb-4hd6))。

---

## 4. 过滤下推：TiDB Coprocessor 把 WHERE 下推到 TiKV 的原理

TiDB 的 SQL 层（TiDB server）与存储层（TiKV）分离，中间是 gRPC Coprocessor 协议。TiDB 把物理计划中可下推的部分（表扫描、索引扫描、过滤 Selection、聚合、TopN）序列化为 protobuf `DagRequest`，连同要扫描的 key ranges、MVCC 读时间戳 `start_ts` 一起发给各 TiKV 节点；TiKV 的 Coprocessor endpoint（`src/coprocessor/endpoint.rs`）在本机存储引擎上执行这段子计划，只把编码后的结果行返回，避免把原始数据全拉回 TiDB([gookv coprocessor 文档](https://github.com/ryogrid/gookv/blob/HEAD/tikv_impl_docs/coprocessor.md))([TiKV dev guide](https://github.com/tikv/tikv-dev-guide/blob/HEAD/src/understanding-tikv/coprocessor/intro.md))。

关键细节有二：① **下推是有条件的**——谓词必须可序列化且语义在两端一致；含用户变量的谓词就不能下推（变量值在执行中可变，TiKV 感知不到），这是官方文档列出的典型反例([TiDB predicate-push-down 文档](https://github.com/pingcap/docs/blob/master/predicate-push-down.md))；② **两端求值语义必须一致**，否则出诡异 bug——真实案例：`ORDER BY` 触发两阶段聚合下推后，TiKV 与 TiDB 对非法日期 CAST 的处理不一致导致结果错误([TiDB issue #69296](https://github.com/pingcap/tidb/issues/69296))。

对 5.2-B"无二级索引只能全表扫描"的缓解思路：不追求完整 Coprocessor，但可以做**简化版"扫描+过滤"下推**——把 `WHERE` 谓词编译成可序列化的小表达式树（如比较/逻辑运算的 AST），随扫描请求发给持有该数据分片的节点，节点在本地 B+ 树扫描时逐行求值，只回传命中的行。这把网络传输从 O(表大小) 降到 O(结果大小)，而实现成本只是一个小型表达式求值器。进一步可下推 `COUNT(*)`/`SUM` 等聚合（只传回一个数）。注意：下推表达式必须禁止非确定函数（`NOW()`、`RAND()`），否则副本结果可能不一致——这是 Calvin 那节确定性要求的回声。

---

## 5. 简化事务：单语句原子性 + BEGIN/COMMIT 做到什么程度 + Calvin 一句话

教学系统的合理目标：**单语句原子性**（一条 INSERT/UPDATE/DELETE 要么全进 Raft 日志、要么全不进——先在内存/暂存区拼好写集，再一次性作为单个 Raft 条目提交）+ **显式 BEGIN/COMMIT 的单分片事务**（多语句、但只触及单个 Raft 组/分片内的数据；提交=一次 2PC-free 的"写事务记录+解析 intent"简化流程）。跨分片事务建议**只做文档说明、不实现**（或实现为"尽力而为、无原子保证"并明确报错），因为真正的分布式 2PC + 故障恢复是数量级更复杂的工程，CockroachDB 的 Parallel Commits 本身就是为驯服 2PC 延迟而生的复杂优化([Medium: serializable transactions](https://medium.com/@ariyanwaliya/inside-cockroachdb-the-engineering-behind-serializable-transactions-8d2946e2de2c))。

隔离级别上，给读做 MVCC 快照读（读 start_ts 快照）、写做"读已提交"即可；串行化是生产系统的默认目标，但对教学系统是可裁剪的奢侈品。

**Calvin 确定性调度思想一句话**：把"对事务顺序达成一致"这件事从执行中抽出来、提前到加锁之前做——先在 sequencing 层给所有事务定一个全局确定顺序并**复制事务的输入而非执行效果**，各副本按同一顺序确定性执行，于是根本不需要分布式提交协议（2PC）和死锁检测([mwhittaker 论文笔记](https://github.com/mwhittaker/papers/blob/HEAD/papers/thomson2012calvin.md))([High Scalability 论文介绍](https://highscalability.com/paper-calvin-fast-distributed-transactions-for-partitioned-d))。

---

## 给 5.2-B 构建的 5 条设计建议

1. **Raft 日志里复制 KV 效果（operation-based），不要复制 SQL 语句**：语句复制要求所有副本确定性重放（`NOW()`、随机数会让副本分叉），效果复制只要求"同样的 KV 写集"，正确性更容易验证。
2. **默认实现"leaseholder 读"**：写主（Raft leader）直接服务读请求、返回本地最新已提交数据；不做 quorum 读、不做 follower 读——单一致性级别先把正确性做对，staleness 读以后再加。
3. **Schema 当带版本号的元数据存在 KV 里，DDL 走"双写过渡"**：如加列分"只写新格式→回填→公开"三阶段推进，各阶段定义好新旧代码的读写可见性，避免混合版本节点互相踩踏。
4. **实现简化版过滤下推**：把 `WHERE` 谓词编译成可序列化的小表达式树发给数据所在节点，节点本地扫描过滤后只回传命中行；禁止下推非确定函数，把网络传输从 O(表大小) 降到 O(结果大小)。
5. **事务做到"单语句原子 + 单分片 BEGIN/COMMIT"为止**：单语句先拼写集再一次性进 Raft；多语句事务限单个分片、提交即一次 intent 解析；跨分片事务明确不支持并报错，不碰分布式 2PC。
