#!/usr/bin/env python3
"""SQL 子集分词器. 纯标准库, 无依赖.

支持的词法元素: 关键字(大小写不敏感)、标识符、整数、字符串单引号('' 转义)、
NULL、括号逗号分号、比较运算符(= <> != < > <= >=)、星号.
"""
import re

KEYWORDS = {
    "CREATE", "TABLE", "PRIMARY", "KEY", "INT", "TEXT", "NOT", "NULL",
    "INSERT", "INTO", "VALUES", "SELECT", "FROM", "WHERE", "UPDATE",
    "SET", "DELETE", "DROP", "AND", "OR", "IS",
}

TOKEN_SPEC = [
    ("NUMBER",   r"-?\d+"),
    ("STRING",   r"'(?:[^']|'')*'"),
    ("OP",       r"<>|!=|<=|>=|=|<|>"),
    ("LPAREN",   r"\("),
    ("RPAREN",   r"\)"),
    ("COMMA",    r","),
    ("SEMI",     r";"),
    ("STAR",     r"\*"),
    ("IDENT",    r"[A-Za-z_][A-Za-z0-9_]*"),
    ("SKIP",     r"[ \t\r\n]+"),
    ("COMMENT",  r"--[^\n]*"),
]
TOKEN_RE = re.compile("|".join(f"(?P<{n}>{p})" for n, p in TOKEN_SPEC))


class LexError(Exception):
    pass


class Token:
    __slots__ = ("kind", "text", "value", "pos")

    def __init__(self, kind, text, value, pos):
        self.kind = kind      # NUMBER/STRING/IDENT/OP/... 或关键字本身如 SELECT
        self.text = text      # 原始文本
        self.value = value    # 解码后的值(int/str/None)
        self.pos = pos

    def __repr__(self):
        return f"Token({self.kind}, {self.text!r})"


def _decode_string(text, pos):
    # text 含两侧单引号, '' 转义为 '
    inner = text[1:-1]
    if "''" in inner:
        inner = inner.replace("''", "'")
    return inner


def tokenize(sql):
    """把 SQL 文本切成 Token 列表(含末尾 EOF). 出错抛 LexError."""
    toks = []
    pos = 0
    n = len(sql)
    while pos < n:
        m = TOKEN_RE.match(sql, pos)
        if not m:
            raise LexError(f"无法识别的字符 {sql[pos]!r} (位置 {pos})")
        kind = m.lastgroup
        text = m.group()
        if kind in ("SKIP", "COMMENT"):
            pos = m.end()
            continue
        if kind == "IDENT" and text.upper() in KEYWORDS:
            kind = text.upper()  # 关键字: kind 即关键字本身
            value = None
        elif kind == "NUMBER":
            value = int(text)
        elif kind == "STRING":
            value = _decode_string(text, pos)
        elif kind == "IDENT":
            value = text
        else:
            value = text
        toks.append(Token(kind, text, value, pos))
        pos = m.end()
    toks.append(Token("EOF", "", None, n))
    return toks
