#!/usr/bin/env python3
"""SQL 正确性测试: 建表/增删改查/WHERE 组合/错误 SQL. 单机直测 TableStore+执行器.

断言计数器 N, 末尾打印总数并要求 >= 60.
usage: python3 tests/test_sql.py
"""
import os
import shutil
import sys
import tempfile

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC)

from sql.lexer import tokenize, LexError
from sql.parser import parse, ParseError
from sql.executor import prepare, apply_plan, query, SQLError, eval_expr
from storage.store import TableStore

N = 0


def check(cond, msg):
    global N
    N += 1
    if not cond:
        raise AssertionError(f"FAIL[{N}]: {msg}")


def check_raises(fn, msg):
    global N
    N += 1
    try:
        fn()
    except (SQLError, ParseError, LexError):
        return
    raise AssertionError(f"FAIL[{N}]: 期望报错但没报: {msg}")


TMP = tempfile.mkdtemp(prefix="capdb-sqltest-")
store = TableStore(os.path.join(TMP, "db"))
_seq = [0]


def get_schema(t):
    txn = store.db.begin("RC")
    try:
        return store.get_schema(txn, t)
    finally:
        txn.abort()


def run(sql):
    """prepare + apply_committed, 返回 result dict."""
    _seq[0] += 1
    plan = prepare(sql, get_schema)
    return store.apply_committed(_seq[0], plan)


def q(sql):
    r = query(store, sql)
    assert r["ok"], r
    return r


# ============ 1. 分词 ============
toks = tokenize("SeLeCT * FROM t WHERE name='o''brien' AND age<=-5;")
check(toks[0].kind == "SELECT", "关键字大小写不敏感")
check(toks[7].kind == "STRING" and toks[7].value == "o'brien", "字符串 '' 转义")
check(toks[11].kind == "NUMBER" and toks[11].value == -5, "负数字面量")
check_raises(lambda: tokenize("SELECT @"), "非法字符报错")
check(tokenize("-- comment\nSELECT 1")[0].kind == "SELECT", "-- 注释跳过")

# ============ 2. 解析 ============
st = parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)")
check(st.table == "t" and st.pk == "id" and len(st.columns) == 3, "CREATE 解析")
st = parse("CREATE TABLE t2 (id INT, v TEXT, PRIMARY KEY (id))")
check(st.pk == "id", "表级 PRIMARY KEY 解析")
st = parse("INSERT INTO t VALUES (1,'a',30),(2,'b',NULL)")
check(len(st.rows) == 2 and st.rows[1][2] is None, "多行 INSERT/NULL 解析")
st = parse("INSERT INTO t (id, name) VALUES (1, 'x')")
check(st.col_names == ["id", "name"], "INSERT 列名表解析")
st = parse("SELECT name, id FROM t WHERE age >= 20 AND NOT name = 'x' OR id <> 1")
check(isinstance(st.where, object) and st.proj == ["name", "id"], "SELECT 解析")
st = parse("UPDATE t SET a=1, b='x' WHERE id <= 3")
check(len(st.assignments) == 2, "UPDATE 多 SET 解析")
st = parse("DELETE FROM t WHERE age IS NOT NULL")
check(st.where is not None, "IS NOT NULL 解析")
st = parse("DROP TABLE t")
check(st.table == "t", "DROP 解析")
check_raises(lambda: parse("CREATE TABLE t (id INT)"), "无主键报错")
check_raises(lambda: parse("CREATE TABLE t (id INT PRIMARY KEY, id TEXT)"),
             "重复列名报错")
check_raises(lambda: parse("SELEC 1"), "坏关键字报错")
check_raises(lambda: parse("SELECT * FROM t WHERE"), "WHERE 悬空报错")
check_raises(lambda: parse("SELECT * FROM t; DROP TABLE t"), "多语句报错")

# ============ 3. 建表 + 插入 ============
r = run("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
check(r["ok"] and r["applied_index"] == _seq[0], "建表 ok, applied 推进")
check_raises(lambda: run("CREATE TABLE users (id INT PRIMARY KEY)"), "重复建表报错")
check_raises(lambda: run("INSERT INTO nosuch VALUES (1)"), "未知表报错")
check_raises(lambda: run("INSERT INTO users VALUES (1,'a')"), "列数不匹配报错")
check_raises(lambda: run("INSERT INTO users VALUES (1,'a','x')"), "类型不匹配报错")
check_raises(lambda: run("INSERT INTO users VALUES (NULL,'a',1)"), "NULL 主键报错")
check_raises(lambda: run("INSERT INTO users (id, nosuch) VALUES (1,'a')"),
             "未知列报错")

r = run("INSERT INTO users VALUES (1,'alice',30),(2,'bob',25),(3,'carol',35),"
        "(4,'dave',NULL),(5,'erin',25)")
check(r["ok"] and r["rows_affected"] == 5, "多行 INSERT")
r = run("INSERT INTO users (id, name) VALUES (6, 'frank')")
check(r["ok"], "列名表 INSERT (缺列补 NULL)")
r = run("INSERT INTO users VALUES (1,'alice2',31)")
check(not r["ok"] and "主键冲突" in r["error"], "主键冲突 -> ok False 的 statement result")
check_raises(lambda: run("INSERT INTO users VALUES (7,'g',1),(7,'h',2)"),
             "语句内主键重复报错")

# ============ 4. SELECT / WHERE ============
r = q("SELECT * FROM users")
check(r["columns"] == ["id", "name", "age"] and len(r["rows"]) == 6, "SELECT *")
r = q("SELECT name, id FROM users WHERE id = 2")
check(r["columns"] == ["name", "id"] and r["rows"] == [["bob", 2]], "列投影顺序")
check(q("SELECT id FROM users WHERE age = 25")["rows"] == [[2], [5]], "WHERE =")
check(q("SELECT id FROM users WHERE age > 30")["rows"] == [[3]], "WHERE >")
check(q("SELECT id FROM users WHERE age >= 30")["rows"] == [[1], [3]], "WHERE >=")
check(q("SELECT id FROM users WHERE age < 30")["rows"] == [[2], [5]], "WHERE <")
check(q("SELECT id FROM users WHERE age <= 25")["rows"] == [[2], [5]], "WHERE <=")
got = q("SELECT id FROM users WHERE age <> 25")["rows"]
check(got == [[1], [3]], f"WHERE <> 排除 NULL 行(dave/frank), 得 {got}")
check(q("SELECT id FROM users WHERE name = 'bob'")["rows"] == [[2]], "字符串 =")
check(q("SELECT id FROM users WHERE name > 'bob'")["rows"] == [[3], [4], [5], [6]],
      "字符串 >")
got = q("SELECT id FROM users WHERE age > 20 AND name <> 'alice'")["rows"]
check(got == [[2], [3], [5]], f"AND, 得 {got}")
got = q("SELECT id FROM users WHERE age < 26 OR name = 'carol'")["rows"]
check(got == [[2], [3], [5]], f"OR, 得 {got}")
got = q("SELECT id FROM users WHERE NOT (age > 30)")["rows"]
check(got == [[1], [2], [4], [5], [6]], f"NOT + NULL 语义, 得 {got}")
check(q("SELECT id FROM users WHERE age IS NULL")["rows"] == [[4], [6]], "IS NULL")
check(len(q("SELECT id FROM users WHERE age IS NOT NULL")["rows"]) == 4, "IS NOT NULL")
got = q("SELECT id FROM users WHERE (age < 26 OR age > 32) AND name <> 'bob'")["rows"]
check(got == [[3], [5]], f"括号优先级, 得 {got}")
check(q("SELECT id FROM users WHERE age = 99")["rows"] == [], "无匹配 -> 空集")
check_raises(lambda: q("SELECT nosuch FROM users"), "SELECT 未知列报错")
check_raises(lambda: q("SELECT id FROM users WHERE nosuch = 1"), "WHERE 未知列报错")
check_raises(lambda: q("SELECT id FROM nosuch"), "SELECT 未知表报错")
check_raises(lambda: query(store, "DELETE FROM users"), "query() 拒绝非 SELECT")

# ============ 5. UPDATE ============
r = run("UPDATE users SET age = 26 WHERE name = 'bob'")
check(r["ok"] and r["rows_affected"] == 1, "UPDATE 单行")
check(q("SELECT age FROM users WHERE id = 2")["rows"] == [[26]], "UPDATE 生效")
r = run("UPDATE users SET age = 40, name = 'CAROL' WHERE id = 3")
check(r["rows_affected"] == 1, "UPDATE 多 SET")
check(q("SELECT name, age FROM users WHERE id = 3")["rows"] == [["CAROL", 40]],
      "多 SET 生效")
r = run("UPDATE users SET age = 99 WHERE age = -99")
check(r["rows_affected"] == 0, "UPDATE 零匹配")
r = run("UPDATE users SET age = 1 WHERE age IS NOT NULL")
check(r["rows_affected"] == 4, "WHERE IS NOT NULL 的 UPDATE (保留 NULL 行)")
check_raises(lambda: run("UPDATE users SET id = 9 WHERE id = 1"), "改主键被拒")
check_raises(lambda: run("UPDATE users SET age = 'x' WHERE id = 1"), "SET 类型错报错")
check_raises(lambda: run("UPDATE users SET nosuch = 1"), "SET 未知列报错")
check_raises(lambda: run("UPDATE nosuch SET a = 1"), "UPDATE 未知表报错")

# ============ 6. DELETE ============
r = run("DELETE FROM users WHERE age IS NULL")
check(r["rows_affected"] == 2, "DELETE WHERE (dave+frank)")
check(q("SELECT id FROM users WHERE id = 4")["rows"] == [], "DELETE 生效")
r = run("DELETE FROM users WHERE id > 100")
check(r["rows_affected"] == 0, "DELETE 零匹配")
check_raises(lambda: run("DELETE FROM nosuch"), "DELETE 未知表报错")

# ============ 7. 字符串转义 / 负数 / TEXT 主键 ============
run("CREATE TABLE files (path TEXT PRIMARY KEY, size INT)")
run("INSERT INTO files VALUES ('o''brien.txt', -12)")
r = q("SELECT size FROM files WHERE path = 'o''brien.txt'")
check(r["rows"] == [[-12]], "转义引号 + 负数 + TEXT 主键")

# ============ 8. DROP TABLE ============
r = run("DROP TABLE files")
check(r["ok"], "DROP TABLE")
check_raises(lambda: q("SELECT * FROM files"), "DROP 后不可查")
check_raises(lambda: run("DROP TABLE files"), "重复 DROP 报错")

# ============ 9. 持久化: 关掉重开, 数据 + applied 游标都在 ============
applied_before = store.get_applied_index()
store.close()
store3 = TableStore(os.path.join(TMP, "db"))
check(store3.get_applied_index() == applied_before, "applied 游标持久化")


def get_schema3(t):
    txn = store3.db.begin("RC")
    try:
        return store3.get_schema(txn, t)
    finally:
        txn.abort()


txn = store3.db.begin("RC")
n = store3.count_rows(txn, "users")
txn.abort()
check(n == 4, f"重启后 4 行都在, 得 {n}")
# 在新句柄上继续 apply: 游标接着走
plan = prepare("INSERT INTO users VALUES (7,'grace',28)", get_schema3)
res = store3.apply_committed(applied_before + 1, plan)
check(res["ok"] and store3.get_applied_index() == applied_before + 1,
      "重启后继续 apply, 游标推进")
txn = store3.db.begin("RC")
n = store3.count_rows(txn, "users")
txn.abort()
check(n == 5, "重启后写入可见")
store3.close()

print(f"\nSQL 正确性: {N} 个断言全部通过 (>= 60: {'OK' if N >= 60 else 'FAIL'})")
assert N >= 60, f"断言数 {N} < 60"
shutil.rmtree(TMP, ignore_errors=True)
