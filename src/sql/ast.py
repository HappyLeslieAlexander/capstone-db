#!/usr/bin/env python3
"""SQL 子集的 AST 节点. 纯数据类, 解析器产出它们, 执行器消费它们."""


class Node:
    def __repr__(self):
        args = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"{type(self).__name__}({args})"


class CreateTable(Node):
    def __init__(self, table, columns, pk):
        self.table = table            # str
        self.columns = columns        # [(name, "INT"/"TEXT")]
        self.pk = pk                  # str, 主键列名


class DropTable(Node):
    def __init__(self, table):
        self.table = table


class Insert(Node):
    def __init__(self, table, col_names, rows):
        self.table = table
        self.col_names = col_names    # None = 按建表顺序; 否则 [col...]
        self.rows = rows              # [[literal...], ...], literal: int/str/None


class Select(Node):
    def __init__(self, table, proj, where):
        self.table = table
        self.proj = proj              # None = *; 否则 [col...]
        self.where = where            # Expr | None


class Update(Node):
    def __init__(self, table, assignments, where):
        self.table = table
        self.assignments = assignments  # [(col, literal)]
        self.where = where              # Expr | None


class Delete(Node):
    def __init__(self, table, where):
        self.table = table
        self.where = where              # Expr | None


# ---- WHERE 表达式 ----
class Expr(Node):
    pass


class BinOp(Expr):
    def __init__(self, op, left, right):
        self.op = op                  # "AND" / "OR"
        self.left = left
        self.right = right


class NotOp(Expr):
    def __init__(self, arg):
        self.arg = arg


class Compare(Expr):
    def __init__(self, op, col, literal):
        self.op = op                  # = < > <= >= <> !=
        self.col = col                # 列名 str
        self.literal = literal        # int/str/None


class IsNull(Expr):
    def __init__(self, col, negated):
        self.col = col
        self.negated = negated        # True = IS NOT NULL
