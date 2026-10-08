#!/usr/bin/env python3
"""执行器: AST -> plan dict -> 在 TableStore 上执行.

plan 是 Raft 日志条目的载荷: 全部 JSON-safe (dict/list/str/int/None),
leader 一次解析, 三个节点 apply 同一份 plan -- 这就是 statement-based
replication 在本项目里的具体形状 (trade-off 见 docs/blogs/01).

两阶段:
  prepare(sql, get_schema): 解析 + 静态校验 (表/列存在、字面量类型) -> plan.
      不碰数据; 失败抛 SQLError, 客户端直接看到, 不进 Raft 日志.
  apply_plan(store, txn, plan): 执行一条已提交日志. 数据相关失败
      (主键冲突、表已存在...) 同样抛 SQLError, 由 TableStore.apply_committed
      记成 {"ok": False} 的 statement result -- 失败的语句本身也是被复制的
      事实, 不是静默丢弃.
非 SQLError 的异常一律上抛 (fail loud, 见 store.py).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sql.lexer import LexError, tokenize  # noqa: E402
from sql.parser import Parser, ParseError  # noqa: E402
from sql.ast import (CreateTable, DropTable, Insert, Select, Update, Delete,  # noqa: E402
                     BinOp, NotOp, Compare, IsNull)


class SQLError(Exception):
    pass


# ---------------- 表达式: AST -> plan dict ----------------
def _expr_to_plan(e):
    if isinstance(e, BinOp):
        return {"op": e.op.lower(), "args": [_expr_to_plan(e.left),
                                            _expr_to_plan(e.right)]}
    if isinstance(e, NotOp):
        return {"op": "not", "arg": _expr_to_plan(e.arg)}
    if isinstance(e, Compare):
        op = "<>" if e.op == "!=" else e.op
        return {"op": op, "col": e.col, "val": e.literal}
    if isinstance(e, IsNull):
        return {"op": "is_not_null" if e.negated else "is_null", "col": e.col}
    raise SQLError(f"不支持的表达式节点: {type(e).__name__}")


def eval_expr(e, row):
    """在行 (dict) 上求 WHERE 表达式的值. NULL 语义: 与 NULL 的比较永假."""
    op = e["op"]
    if op == "and":
        return all(eval_expr(a, row) for a in e["args"])
    if op == "or":
        return any(eval_expr(a, row) for a in e["args"])
    if op == "not":
        return not eval_expr(e["arg"], row)
    if op == "is_null":
        return row.get(e["col"]) is None
    if op == "is_not_null":
        return row.get(e["col"]) is not None
    v = row.get(e["col"])
    lit = e["val"]
    if v is None or lit is None:
        return False  # NULL =/<> 任何值都是 UNKNOWN -> 按假处理; 查 NULL 请用 IS NULL
    if type(v) is not type(lit):
        return False  # 防御: 静态校验已保证字面量与列同类型
    if op == "=":
        return v == lit
    if op == "<>":
        return v != lit
    if op == "<":
        return v < lit
    if op == ">":
        return v > lit
    if op == "<=":
        return v <= lit
    if op == ">=":
        return v >= lit
    raise SQLError(f"未知的比较符: {op}")


# ---------------- 类型检查 ----------------
def _check_type(coltype, val, col):
    if val is None:
        return  # NULL 允许写入任何非主键列 (主键非空在别处检查)
    if coltype == "INT":
        if isinstance(val, int) and not isinstance(val, bool):
            return
    elif coltype == "TEXT":
        if isinstance(val, str):
            return
    raise SQLError(f"列 {col!r} 是 {coltype}, 不能写入值 {val!r}")


def _col_types(schema):
    return {c["name"]: c["type"] for c in schema["columns"]}


# ---------------- prepare: SQL 文本 -> plan ----------------
def prepare(sql, get_schema):
    """解析 + 静态校验, 返回 JSON-safe 的 plan dict."""
    try:
        stmt = Parser(tokenize(sql)).parse_statement()
    except (LexError, ParseError) as e:
        raise SQLError(f"SQL 语法错误: {e}")

    if isinstance(stmt, CreateTable):
        if get_schema(stmt.table) is not None:
            raise SQLError(f"表 {stmt.table!r} 已存在")
        return {"kind": "create_table", "table": stmt.table,
                "columns": [{"name": n, "type": t} for n, t in stmt.columns],
                "pk": stmt.pk}

    if isinstance(stmt, DropTable):
        if get_schema(stmt.table) is None:
            raise SQLError(f"表 {stmt.table!r} 不存在")
        return {"kind": "drop_table", "table": stmt.table}

    if isinstance(stmt, Insert):
        schema = get_schema(stmt.table)
        if schema is None:
            raise SQLError(f"表 {stmt.table!r} 不存在")
        types = _col_types(schema)
        col_order = [c["name"] for c in schema["columns"]]
        pk = schema["pk"]
        if stmt.col_names is not None:
            for c in stmt.col_names:
                if c not in types:
                    raise SQLError(f"表 {stmt.table!r} 没有列 {c!r}")
            if len(set(stmt.col_names)) != len(stmt.col_names):
                raise SQLError("INSERT 列名重复")
        target = stmt.col_names or col_order
        rows = []
        for r in stmt.rows:
            if len(r) != len(target):
                raise SQLError(
                    f"VALUES 列数 {len(r)} 与目标列数 {len(target)} 不一致")
            full = {c: None for c in col_order}
            for c, v in zip(target, r):
                _check_type(types[c], v, c)
                full[c] = v
            if full[pk] is None:
                raise SQLError(f"主键列 {pk!r} 不能为 NULL")
            rows.append([full[c] for c in col_order])
        # 语句内主键去重 (跨语句的冲突在 apply 时检查)
        pks = [r[col_order.index(pk)] for r in rows]
        if len(set(pks)) != len(pks):
            raise SQLError(f"主键 {pk!r} 在一条 INSERT 里重复")
        return {"kind": "insert", "table": stmt.table, "rows": rows}

    if isinstance(stmt, Update):
        schema = get_schema(stmt.table)
        if schema is None:
            raise SQLError(f"表 {stmt.table!r} 不存在")
        types = _col_types(schema)
        pk = schema["pk"]
        assigns = {}
        for c, v in stmt.assignments:
            if c not in types:
                raise SQLError(f"表 {stmt.table!r} 没有列 {c!r}")
            if c == pk:
                raise SQLError("不支持修改主键列 "
                               "(主键是行的身份, 改主键 = 删 + 增, 请显式做)")
            _check_type(types[c], v, c)
            assigns[c] = v
        where = _expr_to_plan(stmt.where) if stmt.where else None
        if where:
            _check_where_cols(where, types, stmt.table)
        return {"kind": "update", "table": stmt.table, "set": assigns,
                "where": where}

    if isinstance(stmt, Delete):
        schema = get_schema(stmt.table)
        if schema is None:
            raise SQLError(f"表 {stmt.table!r} 不存在")
        types = _col_types(schema)
        where = _expr_to_plan(stmt.where) if stmt.where else None
        if where:
            _check_where_cols(where, types, stmt.table)
        return {"kind": "delete", "table": stmt.table, "where": where}

    if isinstance(stmt, Select):
        # SELECT 不进 Raft 日志, 但 prepare 照样做静态校验供 query() 复用
        schema = get_schema(stmt.table)
        if schema is None:
            raise SQLError(f"表 {stmt.table!r} 不存在")
        types = _col_types(schema)
        if stmt.proj is None:
            proj = [c["name"] for c in schema["columns"]]
        else:
            for c in stmt.proj:
                if c not in types:
                    raise SQLError(f"表 {stmt.table!r} 没有列 {c!r}")
            proj = list(stmt.proj)
        where = _expr_to_plan(stmt.where) if stmt.where else None
        if where:
            _check_where_cols(where, types, stmt.table)
        return {"kind": "select", "table": stmt.table, "proj": proj,
                "where": where}

    raise SQLError(f"不支持的语句类型: {type(stmt).__name__}")


def _check_where_cols(e, types, table):
    """WHERE 引用的列必须存在; 比较字面量类型必须与列一致."""
    op = e["op"]
    if op in ("and", "or"):
        for a in e["args"]:
            _check_where_cols(a, types, table)
    elif op == "not":
        _check_where_cols(e["arg"], types, table)
    elif op in ("is_null", "is_not_null"):
        if e["col"] not in types:
            raise SQLError(f"表 {table!r} 没有列 {e['col']!r}")
    else:
        if e["col"] not in types:
            raise SQLError(f"表 {table!r} 没有列 {e['col']!r}")
        _check_type(types[e["col"]], e["val"], e["col"])


# ---------------- apply: plan -> 在 txn 上执行 ----------------
def apply_plan(store, txn, plan):
    """执行一条已提交的 plan, 返回 result dict. 数据错误抛 SQLError."""
    kind = plan["kind"]
    if kind == "create_table":
        store.create_table_txn(txn, plan["table"],
                               [(c["name"], c["type"]) for c in plan["columns"]],
                               plan["pk"])
        return {"ok": True, "rows_affected": 0}
    if kind == "drop_table":
        store.drop_table_txn(txn, plan["table"])
        return {"ok": True, "rows_affected": 0}
    if kind == "insert":
        schema = store.get_schema(txn, plan["table"])
        if schema is None:
            raise SQLError(f"表 {plan['table']!r} 不存在")
        cols = [c["name"] for c in schema["columns"]]
        pk = schema["pk"]
        pk_i = cols.index(pk)
        # 先校验全部主键, 再写任意一行: 失败时无残留
        for r in plan["rows"]:
            if store.get_row(txn, plan["table"], r[pk_i]) is not None:
                raise SQLError(f"主键冲突: {pk}={r[pk_i]!r} 已存在")
        for r in plan["rows"]:
            store.put_row(txn, plan["table"], r[pk_i], dict(zip(cols, r)))
        return {"ok": True, "rows_affected": len(plan["rows"])}
    if kind == "update":
        schema = store.get_schema(txn, plan["table"])
        if schema is None:
            raise SQLError(f"表 {plan['table']!r} 不存在")
        n = 0
        where = plan["where"]
        for pk_val, row in store.scan_rows(txn, plan["table"]):
            if where is None or eval_expr(where, row):
                new = dict(row)
                new.update(plan["set"])
                store.put_row(txn, plan["table"], pk_val, new)
                n += 1
        return {"ok": True, "rows_affected": n}
    if kind == "delete":
        schema = store.get_schema(txn, plan["table"])
        if schema is None:
            raise SQLError(f"表 {plan['table']!r} 不存在")
        where = plan["where"]
        # 先物化: 边扫描边删会破坏叶链迭代
        victims = [pk for pk, row in store.scan_rows(txn, plan["table"])
                   if where is None or eval_expr(where, row)]
        for pk_val in victims:
            store.delete_row(txn, plan["table"], pk_val)
        return {"ok": True, "rows_affected": len(victims)}
    raise SQLError(f"apply 不支持的 plan kind: {kind}")


# ---------------- query: 只读 SELECT ----------------
def query(store, sql):
    """执行 SELECT, 返回 {"columns": [...], "rows": [[...], ...]}.

    纯本地读: 走调用节点的最新已 apply 状态. 一致性取舍见 README
    (未实现 linearizable read, 与上游 Raft skill 一致).
    """
    plan = prepare(sql, lambda t: _schema_of(store, t))
    if plan["kind"] != "select":
        raise SQLError("query() 只接受 SELECT 语句")
    txn = store.db.begin("RC")
    try:
        schema = store.get_schema(txn, plan["table"])
        cols = plan["proj"]
        where = plan["where"]
        rows = []
        for _, row in store.scan_rows(txn, plan["table"]):
            if where is None or eval_expr(where, row):
                rows.append([row.get(c) for c in cols])
        return {"ok": True, "columns": cols, "rows": rows}
    finally:
        txn.abort()  # 只读事务: abort 仅释放, 不写 WAL


def _schema_of(store, table):
    txn = store.db.begin("RC")
    try:
        return store.get_schema(txn, table)
    finally:
        txn.abort()
