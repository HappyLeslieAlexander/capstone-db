#!/usr/bin/env python3
"""SQL 子集的递归下降解析器: Token 流 -> AST. 纯标准库.

文法(EBNF):
  statement  := create_table | drop_table | insert | select | update | delete
  create_table := CREATE TABLE ident '(' table_elem (',' table_elem)* ')'
  table_elem := ident (INT|TEXT) [PRIMARY KEY] | PRIMARY KEY '(' ident ')'
  drop_table := DROP TABLE ident
  insert     := INSERT INTO ident ['(' ident (',' ident)* ')']
                VALUES '(' literal (',' literal)* ')' (',' '(' ... ')')*
  select     := SELECT ('*' | ident (',' ident)*) FROM ident [WHERE expr]
  update     := UPDATE ident SET assign (',' assign)* [WHERE expr]
  assign     := ident '=' literal
  delete     := DELETE FROM ident [WHERE expr]
  expr       := or_expr
  or_expr    := and_expr (OR and_expr)*
  and_expr   := not_expr (AND not_expr)*
  not_expr   := NOT not_expr | predicate
  predicate  := '(' expr ')' | ident IS [NOT] NULL | ident cmp literal
  cmp        := '=' | '<>' | '!=' | '<' | '>' | '<=' | '>='
  literal    := NUMBER | STRING | NULL
"""


class ParseError(Exception):
    pass


class Parser:
    def __init__(self, tokens):
        self.toks = tokens
        self.i = 0

    # ---- 游标 ----
    def peek(self):
        return self.toks[self.i]

    def next(self):
        t = self.toks[self.i]
        self.i += 1
        return t

    def expect(self, kind):
        t = self.peek()
        if t.kind != kind:
            raise ParseError(
                f"期望 {kind}, 但在位置 {t.pos} 看到 {t.text!r}")
        return self.next()

    def accept(self, kind):
        if self.peek().kind == kind:
            return self.next()
        return None

    # ---- 入口 ----
    def parse_statement(self):
        k = self.peek().kind
        if k == "CREATE":
            st = self.parse_create_table()
        elif k == "DROP":
            st = self.parse_drop_table()
        elif k == "INSERT":
            st = self.parse_insert()
        elif k == "SELECT":
            st = self.parse_select()
        elif k == "UPDATE":
            st = self.parse_update()
        elif k == "DELETE":
            st = self.parse_delete()
        else:
            t = self.peek()
            raise ParseError(f"无法识别的语句开头 {t.text!r} (位置 {t.pos})")
        self.accept("SEMI")
        if self.peek().kind != "EOF":
            t = self.peek()
            raise ParseError(f"语句后有多余内容 {t.text!r} (位置 {t.pos})")
        return st

    def ident(self):
        t = self.expect("IDENT")
        return t.value

    # ---- CREATE TABLE ----
    def parse_create_table(self):
        from . import ast as A
        self.expect("CREATE")
        self.expect("TABLE")
        table = self.ident()
        self.expect("LPAREN")
        columns = []
        pk = None
        while True:
            if self.peek().kind == "PRIMARY":
                self.expect("PRIMARY")
                self.expect("KEY")
                self.expect("LPAREN")
                pk2 = self.ident()
                self.expect("RPAREN")
                if pk is not None:
                    raise ParseError("重复定义 PRIMARY KEY")
                pk = pk2
            else:
                name = self.ident()
                t = self.peek()
                if t.kind not in ("INT", "TEXT"):
                    raise ParseError(
                        f"列 {name} 的类型必须是 INT 或 TEXT, 看到 {t.text!r}")
                self.next()
                is_pk = False
                if self.peek().kind == "PRIMARY":
                    self.expect("PRIMARY")
                    self.expect("KEY")
                    is_pk = True
                if any(c == name for c, _ in columns):
                    raise ParseError(f"重复的列名 {name!r}")
                columns.append((name, t.kind))
                if is_pk:
                    if pk is not None:
                        raise ParseError("重复定义 PRIMARY KEY")
                    pk = name
            if self.accept("COMMA"):
                continue
            break
        self.expect("RPAREN")
        if pk is None:
            raise ParseError("CREATE TABLE 必须指定 PRIMARY KEY")
        if pk not in [c for c, _ in columns]:
            raise ParseError(f"PRIMARY KEY {pk!r} 不是已定义的列")
        return A.CreateTable(table, columns, pk)

    # ---- DROP TABLE ----
    def parse_drop_table(self):
        from . import ast as A
        self.expect("DROP")
        self.expect("TABLE")
        return A.DropTable(self.ident())

    # ---- INSERT ----
    def parse_insert(self):
        from . import ast as A
        self.expect("INSERT")
        self.expect("INTO")
        table = self.ident()
        col_names = None
        if self.accept("LPAREN"):
            col_names = [self.ident()]
            while self.accept("COMMA"):
                col_names.append(self.ident())
            self.expect("RPAREN")
        self.expect("VALUES")
        rows = []
        while True:
            self.expect("LPAREN")
            row = [self.literal()]
            while self.accept("COMMA"):
                row.append(self.literal())
            self.expect("RPAREN")
            rows.append(row)
            if self.accept("COMMA"):
                continue
            break
        if not rows:
            raise ParseError("VALUES 后至少要有一行")
        return A.Insert(table, col_names, rows)

    # ---- SELECT ----
    def parse_select(self):
        from . import ast as A
        self.expect("SELECT")
        if self.accept("STAR"):
            proj = None
        else:
            proj = [self.ident()]
            while self.accept("COMMA"):
                proj.append(self.ident())
        self.expect("FROM")
        table = self.ident()
        where = None
        if self.accept("WHERE"):
            where = self.parse_expr()
        return A.Select(table, proj, where)

    # ---- UPDATE ----
    def parse_update(self):
        from . import ast as A
        self.expect("UPDATE")
        table = self.ident()
        self.expect("SET")
        assigns = [(self.ident(), self._expect_eq_literal())]
        while self.accept("COMMA"):
            assigns.append((self.ident(), self._expect_eq_literal()))
        where = None
        if self.accept("WHERE"):
            where = self.parse_expr()
        return A.Update(table, assigns, where)

    def _expect_eq_literal(self):
        self.expect("OP")  # 必须是 '='
        t = self.toks[self.i - 1]
        if t.text != "=":
            raise ParseError(f"SET 后只支持 '=', 看到 {t.text!r}")
        return self.literal()

    # ---- DELETE ----
    def parse_delete(self):
        from . import ast as A
        self.expect("DELETE")
        self.expect("FROM")
        table = self.ident()
        where = None
        if self.accept("WHERE"):
            where = self.parse_expr()
        return A.Delete(table, where)

    # ---- 字面量 ----
    def literal(self):
        t = self.peek()
        if t.kind == "NUMBER":
            self.next()
            return t.value
        if t.kind == "STRING":
            self.next()
            return t.value
        if t.kind == "NULL":
            self.next()
            return None
        raise ParseError(f"期望字面量(数字/字符串/NULL), 看到 {t.text!r}")

    # ---- WHERE 表达式 ----
    def parse_expr(self):
        return self.parse_or()

    def parse_or(self):
        from . import ast as A
        left = self.parse_and()
        while self.accept("OR"):
            left = A.BinOp("OR", left, self.parse_and())
        return left

    def parse_and(self):
        from . import ast as A
        left = self.parse_not()
        while self.accept("AND"):
            left = A.BinOp("AND", left, self.parse_not())
        return left

    def parse_not(self):
        from . import ast as A
        if self.accept("NOT"):
            return A.NotOp(self.parse_not())
        return self.parse_predicate()

    def parse_predicate(self):
        from . import ast as A
        if self.accept("LPAREN"):
            e = self.parse_expr()
            self.expect("RPAREN")
            return e
        col = self.ident()
        t = self.peek()
        if t.kind == "IS":
            self.next()
            neg = bool(self.accept("NOT"))
            self.expect("NULL")
            return A.IsNull(col, neg)
        if t.kind == "OP":
            self.next()
            if t.text not in ("=", "<>", "!=", "<", ">", "<=", ">="):
                raise ParseError(f"不支持的比较符 {t.text!r}")
            return A.Compare(t.text, col, self.literal())
        raise ParseError(
            f"WHERE 条件在列 {col!r} 后期望比较符或 IS NULL, 看到 {t.text!r}")


def parse(sql):
    """解析一条 SQL, 返回 AST. 失败抛 ParseError / LexError."""
    from .lexer import tokenize
    return Parser(tokenize(sql)).parse_statement()
