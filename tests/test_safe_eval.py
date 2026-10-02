"""
test_safe_eval.py — `custom` metric expressions run through an AST
whitelist: the documented pandas idioms work, anything that could reach
the interpreter (imports, dunders, open(), lambdas, writers) is rejected
before a single node is evaluated.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from datadelta.metrics   import MetricDefinition, MetricsConfig, evaluate_all_metrics
from datadelta.safe_eval import (
    ALLOWED_BUILTINS,
    ALLOWED_NP,
    ALLOWED_PD,
    UnsafeExpressionError,
    safe_eval,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({
        "revenue": [500.0, 1500.0, 2500.0, 800.0],
        "a":       [1.0, 2.0, 3.0, 4.0],
        "b":       [2.0, 2.0, 2.0, 2.0],
        "s":       ["X", "y", "x", "Z"],
        "d":       ["2023-12-31", "2024-01-02", "2024-02-01", "2023-06-01"],
        "plan":    ["free", "pro", "free", "team"],
    })


# ── Whitelist constants (contract) ────────────────────────────────────────────

def test_whitelist_constants_match_the_contract():
    assert ALLOWED_NP == frozenset({"log", "log1p", "sqrt", "abs", "where", "nan"})
    assert ALLOWED_PD == frozenset({"to_datetime", "Timestamp", "Timedelta"})
    assert set(ALLOWED_BUILTINS) == {"abs", "len", "min", "max", "round", "float", "int"}
    assert issubclass(UnsafeExpressionError, ValueError)


# ── Allowed expressions ───────────────────────────────────────────────────────

@pytest.mark.parametrize("expression, expected", [
    ("(df['revenue'] > 1000).mean()",                                    0.5),
    ("df['a'].sum() / df['b'].sum()",                                    1.25),
    ("df['s'].str.lower().isin(['x']).mean()",                           0.5),
    ("np.log1p(df['a']).mean()",                                         float(np.log1p([1.0, 2.0, 3.0, 4.0]).mean())),
    ("(pd.to_datetime(df['d']) > pd.Timestamp('2024-01-01')).mean()",   0.5),
    ("(pd.to_datetime(df['d']).dt.year == 2024).mean()",                 0.5),
    ("(df['plan'] != 'free').mean()",                                    0.5),
    ("len(df)",                                                          4),
    ("df['a'].astype(float).clip(0, 2).max()",                           2.0),
    ("round(df['a'].quantile(0.5), 1)",                                  2.5),
    ("np.where(df['a'] > 2, 1, 0).mean()",                               0.5),
    ("(df['a'].notna() & (df['b'] == 2)).mean()",                        1.0),
    ("df['a'][1:3].sum()",                                               5.0),
    ("df[['a', 'b']].sum().sum()",                                       18.0),
    ("-df['a'].min() + abs(-3)",                                         2.0),
])
def test_allowed_expressions_evaluate(df, expression, expected):
    assert float(safe_eval(expression, df)) == pytest.approx(expected)


def test_np_nan_is_allowed(df):
    assert np.isnan(safe_eval("np.nan", df))


def test_numpy_reductions_work_in_a_fresh_interpreter(tmp_path, subprocess_env):
    """
    ndarray.mean/.std import numpy._core._methods lazily, from C, through the
    eval scope's __builtins__. In this test process another case may already
    have warmed that import, so run the expressions in a fresh interpreter.
    """
    code = (
        "import pandas as pd\n"
        "from datadelta.safe_eval import safe_eval\n"
        "df = pd.DataFrame({'a': [1.0, 2.0, 3.0, 4.0]})\n"
        "print(safe_eval(\"np.where(df['a'] > 2, 1, 0).mean()\", df),"
        " safe_eval(\"np.where(df['a'] > 2, 1, 0).std()\", df))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output = True,
        text           = True,
        cwd            = tmp_path,
        env            = subprocess_env,
        timeout        = 60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["0.5", "0.5"]


def test_eval_scope_can_only_import_numpy_and_pandas():
    """The one builtin in the eval scope is an importer limited to numpy/pandas."""
    from datadelta.safe_eval import _restricted_import

    assert _restricted_import("numpy.linalg", fromlist=("norm",)).__name__ == "numpy.linalg"
    for name in ("os", "subprocess", "pathlib", "numpyx"):
        with pytest.raises(ImportError, match="not allowed"):
            _restricted_import(name)
    with pytest.raises(ImportError, match="not allowed"):
        _restricted_import("numpy", level=1)


def test_example_metrics_files_stay_valid():
    """Every `custom` expression shipped in examples/*.yaml passes the whitelist."""
    frame = pd.DataFrame({"plan": ["free", "pro"], "revenue": [10.0, 2000.0]})
    expressions = []
    for path in sorted((PROJECT_ROOT / "examples").glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        expressions += [m["expression"] for m in raw.get("metrics", []) if m.get("type") == "custom"]
    assert expressions, "expected at least one custom metric in examples/"
    for expression in expressions:
        assert 0.0 <= float(safe_eval(expression, frame)) <= 1.0


# ── Rejected expressions ──────────────────────────────────────────────────────

@pytest.mark.parametrize("expression", [
    "__import__('os').system('echo hi')",
    "().__class__.__bases__",
    "df.__class__",
    "open('x')",
    "(lambda: 1)()",
    "[x for x in df]",
    "df.to_csv('x')",
    "getattr(df, 'x')",
    "np.load('x')",
    "pd.read_csv('x')",
    "df.eval('a + 1')",
    "df.query('a > 1')",
    "df['a'].apply(len)",
    "df.pipe(len)",
    "int.__subclasses__()",
    "df.mean.__globals__",
    "f'{df}'",
    "{'a': 1}",
    "{1, 2}",
    "(y := 1)",
    "max(*df)",
    "df['a'].sum(**{'axis': 0})",
    "np",
    "pd",
    "df['a'].sum() if True else 0",
])
def test_unsafe_expressions_are_rejected(df, isolated_cwd, expression):
    with pytest.raises(UnsafeExpressionError, match="not allowed"):
        safe_eval(expression, df)
    # Nothing was evaluated: df.to_csv('x') / open('x') left no file behind.
    assert list(isolated_cwd.iterdir()) == []


@pytest.mark.parametrize("expression, offender", [
    ("(lambda: 1)()",                       "Lambda"),
    ("df.to_csv('x')",                      "to_csv"),
    ("open('x')",                           "open"),
    ("__import__('os')",                    "__import__"),
    ("df.__class__",                        "__class__"),
    ("np.load('x')",                        "np.load"),
    ("[x for x in df]",                     "ListComp"),
])
def test_error_message_names_the_offending_construct(df, expression, offender):
    with pytest.raises(UnsafeExpressionError) as excinfo:
        safe_eval(expression, df)
    assert offender in str(excinfo.value)


def test_validation_happens_before_evaluation(df):
    # If evaluated, open('missing.txt') would raise FileNotFoundError first.
    with pytest.raises(UnsafeExpressionError):
        safe_eval("df['a'].sum() + open('missing.txt').read()", df)


def test_syntax_errors_are_value_errors(df):
    with pytest.raises(ValueError, match="cannot parse"):
        safe_eval("df[", df)


def test_non_string_expression_is_a_value_error(df):
    with pytest.raises(ValueError, match="must be a string"):
        safe_eval(42, df)


# ── metrics.py integration ────────────────────────────────────────────────────

def _custom(expression: str) -> MetricsConfig:
    return MetricsConfig(metrics=[MetricDefinition(
        name        = "probe",
        description = "custom probe",
        type        = "custom",
        expression  = expression,
    )])


def test_custom_metric_uses_safe_eval(df):
    after = df.assign(revenue=[1500.0, 1500.0, 2500.0, 800.0])
    findings = evaluate_all_metrics(df, after, _custom("(df['revenue'] > 1000).mean()"))
    assert len(findings) == 1
    assert findings[0].layer == "custom"
    assert findings[0].metric["value_before"] == 0.5
    assert findings[0].metric["value_after"]  == 0.75


SIDE_EFFECTS = [
    "__import__('pathlib').Path('sentinel.txt').touch()",
    "open('sentinel.txt', 'w').write('pwned')",
]


@pytest.mark.parametrize("expression", SIDE_EFFECTS)
# The open(...).write(...) probe deliberately leaks its file handle; keep the
# run quiet under `-W error` too.
@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_sentinel_expressions_really_have_side_effects(isolated_cwd, expression):
    """Control: with plain eval() these expressions do create the sentinel file."""
    eval(expression)  # noqa: S307 — deliberate, proves the sentinel works
    assert (isolated_cwd / "sentinel.txt").exists()


@pytest.mark.parametrize("expression", SIDE_EFFECTS)
def test_unsafe_custom_metric_becomes_a_warn_without_running(df, isolated_cwd, expression):
    findings = evaluate_all_metrics(df, df, _custom(expression))
    assert len(findings) == 1
    assert findings[0].severity == "WARN"
    assert "not allowed" in findings[0].title
    assert not (isolated_cwd / "sentinel.txt").exists()
