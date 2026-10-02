"""
safe_eval.py — Evaluate `custom` metric expressions through an AST whitelist.

WHY NOT eval()?
  metrics.yaml is meant to be committed and shared: a teammate edits it,
  a pull request changes it, CI runs `datadelta diff` against it. With a
  bare eval(), one line such as
      expression: "__import__('os').system('curl ... | sh')"
  runs arbitrary code on every machine and CI runner that diffs with the
  file. A custom metric only needs a small slice of pandas, so we allow
  exactly that slice and reject everything else BEFORE evaluating.

HOW IT WORKS:
  1. ast.parse(expression, mode="eval") turns the string into a tree.
  2. _Validator walks every node. Allowed node types are listed in
     _ALLOWED_NODES; anything else (lambda, comprehensions, f-strings,
     dict/set displays, walrus, *args/**kwargs, if-expressions) raises
     UnsafeExpressionError naming the offending construct.
  3. Names: `df` and the whitelisted builtins may appear anywhere; `np`
     and `pd` only as `np.<fn>` / `pd.<fn>` with <fn> in ALLOWED_NP /
     ALLOWED_PD. Every other attribute must be in ALLOWED_ATTRS, and any
     name, attribute or keyword starting with "_" is rejected — that
     closes the classic `().__class__.__bases__[0].__subclasses__()`
     escape.
  4. Calls: only whitelisted builtins (by name) and whitelisted
     methods/functions (by attribute) can be called.
  5. The validated tree is compiled and evaluated with a __builtins__
     that holds nothing but _restricted_import, so even a validator bug
     cannot reach open(), eval() or an import of anything outside
     numpy/pandas.

  WHY __builtins__ IS NOT SIMPLY EMPTY:
    numpy imports some of its own implementation modules lazily, from C,
    on first use: ndarray.mean / .std / .var load numpy._core._methods.
    That import looks up __import__ in the builtins of the frame that is
    running, i.e. our eval scope. With an empty __builtins__ the allowed
    expression  np.where(df['a'] > 2, 1, 0).mean()  raised
    KeyError: '__import__' in a fresh process. _restricted_import only
    loads numpy/pandas modules, and an expression can never name it
    (names starting with "_" are rejected by the validator).

  Example — allowed:   (df['revenue'] > 1000).mean()
  Example — rejected:  df.to_csv('x')   →  attribute 'to_csv' is not allowed
"""

from __future__ import annotations

import ast
import builtins as _builtins
from typing import Any, Callable

import numpy as np
import pandas as pd


class UnsafeExpressionError(ValueError):
    """The expression uses a construct outside the custom-metric whitelist."""


# ─────────────────────────────────────────────────────────────────────────────
# Whitelist
# ─────────────────────────────────────────────────────────────────────────────

# Methods / attributes allowed after any value (Series, DataFrame, accessor,
# scalar). Only aggregations, element-wise predicates and pure transforms:
# nothing that writes files, evaluates strings, or calls user functions.
ALLOWED_ATTRS: frozenset[str] = frozenset({
    # aggregations
    "mean", "sum", "count", "nunique", "median", "std", "var", "min", "max",
    "quantile", "size", "shape",
    # element-wise
    "abs", "round", "isna", "notna", "isnull", "notnull", "fillna",
    "between", "isin", "astype", "clip", "dropna",
    # string accessor
    "str", "contains", "startswith", "endswith", "lower", "upper", "strip", "len",
    # datetime accessor / Timedelta
    "dt", "days", "year", "month", "day", "dayofweek",
})

ALLOWED_NP: frozenset[str] = frozenset({"log", "log1p", "sqrt", "abs", "where", "nan"})

ALLOWED_PD: frozenset[str] = frozenset({"to_datetime", "Timestamp", "Timedelta"})

ALLOWED_BUILTINS: dict[str, Callable] = {
    "abs":   abs,
    "len":   len,
    "min":   min,
    "max":   max,
    "round": round,
    "float": float,
    "int":   int,
}

# AST node types an expression may contain. Operator classes such as
# ast.Add and ast.Gt are listed too: the visitor reaches them as child nodes.
_ALLOWED_NODES: tuple[type, ...] = (
    ast.Expression, ast.Constant, ast.Name, ast.Load,
    ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare,
    ast.Subscript, ast.Attribute, ast.Call, ast.keyword,
    ast.Tuple, ast.List, ast.Slice,
    # arithmetic and bitwise operators (& | ~ build pandas boolean masks)
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.BitAnd, ast.BitOr, ast.BitXor, ast.Invert, ast.Not, ast.UAdd, ast.USub,
    ast.And, ast.Or,
    # comparisons
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn, ast.Is, ast.IsNot,
)

# Bare names usable anywhere. np / pd are module objects and are only
# accepted as the base of a whitelisted attribute (np.log, pd.Timestamp).
_BARE_NAMES: frozenset[str] = frozenset({"df", *ALLOWED_BUILTINS})
_MODULES:    dict[str, frozenset[str]] = {"np": ALLOWED_NP, "pd": ALLOWED_PD}


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

class _Validator(ast.NodeVisitor):
    """Raise UnsafeExpressionError on the first node outside the whitelist."""

    def generic_visit(self, node: ast.AST) -> None:
        if not isinstance(node, _ALLOWED_NODES):
            raise UnsafeExpressionError(
                f"{type(node).__name__} is not allowed in custom metric expressions"
            )
        super().generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        _reject_private(node.id, "name")
        if node.id in _MODULES:
            raise UnsafeExpressionError(
                f"name '{node.id}' is not allowed on its own; use {node.id}.<function>"
            )
        if node.id not in _BARE_NAMES:
            raise UnsafeExpressionError(
                f"name '{node.id}' is not allowed "
                f"(allowed: df, np.<fn>, pd.<fn>, {', '.join(sorted(ALLOWED_BUILTINS))})"
            )

    def visit_Attribute(self, node: ast.Attribute) -> None:
        _reject_private(node.attr, "attribute")
        base = node.value
        if isinstance(base, ast.Name) and base.id in _MODULES:
            allowed = _MODULES[base.id]
            if node.attr not in allowed:
                raise UnsafeExpressionError(
                    f"{base.id}.{node.attr} is not allowed "
                    f"(allowed: {', '.join(f'{base.id}.{a}' for a in sorted(allowed))})"
                )
            return   # the module name itself is fine in this position
        if node.attr not in ALLOWED_ATTRS:
            raise UnsafeExpressionError(f"attribute '{node.attr}' is not allowed")
        self.visit(base)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name):
            if func.id not in ALLOWED_BUILTINS:
                _reject_private(func.id, "name")
                raise UnsafeExpressionError(
                    f"calling '{func.id}' is not allowed "
                    f"(allowed functions: {', '.join(sorted(ALLOWED_BUILTINS))})"
                )
        elif not isinstance(func, ast.Attribute):
            raise UnsafeExpressionError(
                f"calling a {type(func).__name__} is not allowed; "
                f"only whitelisted functions and methods can be called"
            )
        self.generic_visit(node)

    def visit_keyword(self, node: ast.keyword) -> None:
        if node.arg is None:
            raise UnsafeExpressionError("**kwargs unpacking is not allowed")
        _reject_private(node.arg, "keyword")
        self.generic_visit(node)


def _reject_private(identifier: str, kind: str) -> None:
    if identifier.startswith("_"):
        raise UnsafeExpressionError(
            f"{kind} '{identifier}' is not allowed (names starting with '_' are blocked)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Evaluation
# ─────────────────────────────────────────────────────────────────────────────

# Top-level packages whose submodules may be imported during evaluation.
_IMPORTABLE_PACKAGES: frozenset[str] = frozenset({"numpy", "pandas"})


def _restricted_import(
    name:     str,
    globals:  dict | None = None,
    locals:   dict | None = None,
    fromlist: tuple       = (),
    level:    int         = 0,
) -> Any:
    """
    The only entry of the eval scope's __builtins__. numpy/pandas import
    their own submodules lazily from C (ndarray.mean -> numpy._core._methods),
    and that lookup uses the evaluating frame's __builtins__. Only those
    packages are allowed. Expressions can never name __import__: names
    starting with '_' are rejected by the validator.
    """
    if level != 0 or name.partition(".")[0] not in _IMPORTABLE_PACKAGES:
        raise ImportError(f"import of {name!r} is not allowed in custom metric expressions")
    return _builtins.__import__(name, globals, locals, fromlist, level)


def safe_eval(expression: str, df: pd.DataFrame) -> Any:
    """
    Validate `expression` against the whitelist, then evaluate it with
    `df`, `np`, `pd` and the whitelisted builtins in scope.

    Raises UnsafeExpressionError (a ValueError) for a disallowed construct
    and ValueError for an expression that is not a string or does not parse.
    Nothing is evaluated unless the whole tree validates.
    """
    if not isinstance(expression, str):
        raise ValueError(
            f"custom expression must be a string, got {type(expression).__name__}"
        )
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as e:
        raise ValueError(f"cannot parse expression: {e.msg}") from None

    _Validator().visit(tree)

    code  = compile(tree, "<metrics.yaml expression>", "eval")
    scope = {
        "__builtins__": {"__import__": _restricted_import},
        "df": df, "np": np, "pd": pd,
        **ALLOWED_BUILTINS,
    }
    return eval(code, scope)  # noqa: S307 — tree validated against the whitelist above
