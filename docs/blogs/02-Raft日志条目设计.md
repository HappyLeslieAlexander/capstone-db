# 把 Raft 日志条目设计成什么样：写操作如何序列化与 apply

> 草稿。只写草稿，不发布。

## 日志条目长什么样

一条写 SQL 进 Raft 日志时，长这样 (JSON)：

```json
{
  "term": 3,
  "op": {
    "op": "sql",
    "stmt": {
      "kind": "insert",
      "table": "users",
      "rows": [[1, "alice", 30], [2, "bob", 25]]
    }
  }
}
```

`stmt` 就是 plan dict —— SQL 解析器输出的、JSON 可序列化的结构。
SELECT 的 plan 长这样：

```json
{
  "kind": "select",
  "table": "users",
  "columns": ["id", "name"],
  "where": {"op": "and",
            "left": {"op": ">", "col": "age", "val": 20},
            "right": {"op": "=", "col": "name", "val": "alice"}}
}
```

WHERE 被编译成表达式树，`{"op": ">", "col": "age", "val": 20}` 这种。
这棵树是**纯数据**，可以 JSON 序列化、可以网络传输、可以在任何节点上
求值 —— 备忘录说的"过滤下推"的前提条件，我已经满足了 (只是现在没分片，
用不上)。

## 写路径：五步

1. **解析**：客户端把 SQL 文本 POST 到 `/exec`。节点在**不持有 Raft 锁**
   的情况下做解析 (解析可能慢，不能卡住心跳)。
2. **分类**：SELECT 走读路径 (见下)；写语句生成 plan dict。
3. **进日志**：leader 把 `{"op": "sql", "stmt": plan}` 追加到 Raft 日志，
   复制到多数派。
4. **提交**：多数派确认后，`commit_index` 推进。
5. **Apply**：每个节点 (包括 leader 自己) 按日志顺序执行
   `store.apply_committed(index, plan)`，把效果写进本地 B+ 树，
   并把 `applied` 游标 (已应用的最大 index) **和语句效果在同一个
   MiniDB 事务里提交**。

第 5 步的"同事务"是关键，下一篇讲。

## 读路径：两条

- **Leader**：直接读本地已提交数据。Raft 保证已提交的日志不会丢，
  leader 的本地状态就是"已提交"的物化。
- **Follower**：把 SELECT 转发给 leader (HTTP，带 `forwarded` 标记防循环)。
  如果不知道 leader 是谁，或转发失败，降级为本地读，返回里带
  `stale_read_possible: true`。

## 语句失败怎么办？

比如主键冲突：`INSERT` 了一个已存在的 id。

这个失败**也是 Raft 日志里的一条记录**。所有节点都会重放它，都会得到
"主键冲突"的结果。`applied` 游标照常推进 —— 失败的语句本身是一个被复制
的事实，不是静默丢弃。

客户端收到的响应是 `{"ok": false, "error": "..."}`，**不带 leader hint**。
客户端看到"有 error 无 leader"就知道：这是语句本身的问题，重试也没用，
不要换个节点再试。

## 为什么 plan 里要有表结构快照？

`INSERT INTO users VALUES (1, 'alice', 30)` 的 plan 里，我不存列名，
只存值。执行时靠 catalog 查列定义。

如果 `DROP TABLE users` 和 `INSERT` 并发呢？Raft 日志是全局有序的，
先到的先生效，不存在"执行时表没了"的竞态 —— 因为执行顺序就是日志顺序，
而日志顺序是确定的。这就是"语句复制"在单机引擎上的优雅之处：
**并发控制交给了 Raft 的日志顺序，引擎只需要保证单条语句原子**。
