"""
plan.py — What the cleaner is allowed to do, validated before it runs.

PRECEDENCE
  built-in defaults  <  `cleaning:` in metrics.yaml  <  CLI flags

  The defaults are lossless: headers, whitespace, null tokens, booleans,
  numbers and dates run, but numbers/dates convert a column only when
  every non-null value parses (min_parse_ratio 1.0). case, dedupe and
  impute change or remove information, so they run only when configured.

WHY STRICT VALIDATION?
  A typo such as `dedupe: {mode: keys}` or `nul_tokens:` would otherwise
  be ignored silently and the user would believe a rule ran. Every
  unknown key and every invalid value raises CleanConfigError with the
  full key path, e.g. "cleaning.numbers.min_parse_ratio: expected a
  number in (0, 1], got 1.5". The CLI turns that into exit code 1.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib     import Path
from typing      import Any, Literal, NoReturn, Union

import yaml


# The file `datadelta diff` already auto-discovers for metrics; the
# optional top-level `cleaning:` key lives in the same file.
DEFAULT_CONFIG_FILE = "metrics.yaml"

# Compared after strip() + casefold(), so "N/A", " n/a " and "#N/A" match.
DEFAULT_NULL_TOKENS: tuple[str, ...] = ("", "na", "n/a", "nan", "null", "none", "-", "—", "--", "#n/a")

CASE_MODES:         tuple[str, ...] = ("lower", "upper", "title")
DEDUPE_MODES:       tuple[str, ...] = ("exact", "key")
KEEP_CHOICES:       tuple[str, ...] = ("first", "last")
IMPUTE_METHODS:     tuple[str, ...] = ("median", "mean", "mode", "drop_rows")
CLI_IMPUTE_CHOICES: tuple[str, ...] = ("median", "mode")


class CleanConfigError(ValueError):
    """Invalid cleaning configuration. The message starts with the key path."""


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ParseOptions:
    min_parse_ratio: float = 1.0


@dataclass
class DateOptions:
    min_parse_ratio: float = 1.0
    dayfirst:        bool  = False


@dataclass
class DedupeOptions:
    mode: Literal["exact", "key"]
    keys: list[str] = field(default_factory=list)
    keep: Literal["first", "last"] = "first"


ImputeRule = Union[str, dict]   # "median" | "mean" | "mode" | "drop_rows" | {"constant": value}


@dataclass
class CleanPlan:
    exclude_columns: list[str] = field(default_factory=list)
    headers:         bool = True
    whitespace:      bool = True
    null_tokens:     list[str] | None = field(default_factory=lambda: list(DEFAULT_NULL_TOKENS))  # None = step disabled
    booleans:        bool = True
    numbers:         ParseOptions | None = field(default_factory=ParseOptions)                    # None = disabled
    dates:           DateOptions | None = field(default_factory=DateOptions)                      # None = disabled
    case:            dict[str, list[str]] = field(default_factory=dict)      # keys ⊆ {"lower","upper","title"}
    dedupe:          DedupeOptions | None = None
    impute:          dict[str, ImputeRule] = field(default_factory=dict)
    impute_default:  Literal["median", "mode"] | None = None                 # from --impute
    key_column:      str | None = None                                       # for the duplicate_keys stat (None = auto-detect)
    keep_text:       list[str] = field(default_factory=list)                 # diff --clean only, not a YAML key: columns
                                                                             # the parse steps leave as text (clean_both)


# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fail(path: str, problem: str) -> NoReturn:
    raise CleanConfigError(f"{path}: {problem}")


def _describe(value: Any) -> str:
    """How a YAML value is quoted in error messages."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "a list"
    if isinstance(value, dict):
        return "a mapping"
    return type(value).__name__


def _mapping(value: Any, path: str) -> dict:
    if not isinstance(value, dict):
        _fail(path, f"expected a mapping, got {_describe(value)}")
    return value


def _check_keys(mapping: dict, allowed: tuple[str, ...], path: str) -> None:
    for key in mapping:
        if key not in allowed:
            _fail(f"{path}.{key}", f"unknown key (expected one of: {', '.join(allowed)})")


def _bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(path, f"expected true or false, got {_describe(value)}")
    return value


def _name(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(path, f"expected a column name (string), got {_describe(value)}")
    return value


def _names(value: Any, path: str) -> list[str]:
    if not isinstance(value, list):
        _fail(path, f"expected a list of column names, got {_describe(value)}")
    return [_name(item, f"{path}[{i}]") for i, item in enumerate(value)]


def _ratio(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not (0 < value <= 1):
        _fail(path, f"expected a number in (0, 1], got {_describe(value)}")
    return float(value)


# ── One parser per top-level key ──────────────────────────────────────────────

def _null_tokens(value: Any, path: str) -> list[str] | None:
    if value is False:
        return None
    if value is True:
        return list(DEFAULT_NULL_TOKENS)
    if not isinstance(value, list):
        _fail(path, f"expected a list of strings or false, got {_describe(value)}")
    for i, token in enumerate(value):
        if not isinstance(token, str):
            _fail(f"{path}[{i}]", f"expected a string, got {_describe(token)} (quote it in YAML)")
    return list(value)


def _parse_options(value: Any, path: str) -> ParseOptions | None:
    if value is False:
        return None
    if value is True:
        return ParseOptions()
    options = _mapping(value, path)
    _check_keys(options, ("min_parse_ratio",), path)
    return ParseOptions(
        min_parse_ratio = _ratio(options.get("min_parse_ratio", 1.0), f"{path}.min_parse_ratio"),
    )


def _date_options(value: Any, path: str) -> DateOptions | None:
    if value is False:
        return None
    if value is True:
        return DateOptions()
    options = _mapping(value, path)
    _check_keys(options, ("min_parse_ratio", "dayfirst"), path)
    return DateOptions(
        min_parse_ratio = _ratio(options.get("min_parse_ratio", 1.0), f"{path}.min_parse_ratio"),
        dayfirst        = _bool(options.get("dayfirst", False), f"{path}.dayfirst"),
    )


def _case(value: Any, path: str) -> dict[str, list[str]]:
    modes = _mapping(value, path)
    _check_keys(modes, CASE_MODES, path)
    out:  dict[str, list[str]] = {}
    seen: dict[str, str] = {}
    for mode, columns in modes.items():
        names = _names(columns, f"{path}.{mode}")
        for name in names:
            if name in seen and seen[name] != mode:
                _fail(path, f"column {name!r} is listed under both {seen[name]} and {mode}")
            seen[name] = mode
        out[mode] = names
    return out


def _dedupe(value: Any, path: str) -> DedupeOptions | None:
    if value is False:
        return None
    options = _mapping(value, path)
    _check_keys(options, ("mode", "keys", "keep"), path)
    if "mode" not in options:
        _fail(f"{path}.mode", "required (exact or key)")
    mode = options["mode"]
    if mode not in DEDUPE_MODES:
        _fail(f"{path}.mode", f"expected exact or key, got {_describe(mode)}")
    keys = _names(options.get("keys", []), f"{path}.keys")
    keep = options.get("keep", "first")
    if keep not in KEEP_CHOICES:
        _fail(f"{path}.keep", f"expected first or last, got {_describe(keep)}")
    if mode == "key" and not keys:
        _fail(f"{path}.keys", "required when mode is key")
    if mode == "exact" and keys:
        _fail(f"{path}.keys", "only allowed when mode is key")
    return DedupeOptions(mode=mode, keys=keys, keep=keep)


def _impute(value: Any, path: str) -> dict[str, ImputeRule]:
    columns = _mapping(value, path)
    rules: dict[str, ImputeRule] = {}
    for column, rule in columns.items():
        if not isinstance(column, str) or not column:
            _fail(path, f"column name {_describe(column)} must be a string (quote it in YAML)")
        rule_path = f"{path}.{column}"
        if isinstance(rule, str):
            if rule not in IMPUTE_METHODS:
                _fail(rule_path, f"expected median, mean, mode, drop_rows or {{constant: value}}, got {_describe(rule)}")
            rules[column] = rule
        elif isinstance(rule, dict):
            _check_keys(rule, ("constant",), rule_path)
            if "constant" not in rule:
                _fail(f"{rule_path}.constant", "required")
            constant = rule["constant"]
            if constant is None or isinstance(constant, (list, dict)):
                _fail(f"{rule_path}.constant", f"expected a number, string or boolean, got {_describe(constant)}")
            rules[column] = {"constant": constant}
        else:
            _fail(rule_path, f"expected median, mean, mode, drop_rows or {{constant: value}}, got {_describe(rule)}")
    return rules


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

_TOP_KEYS: tuple[str, ...] = (
    "exclude_columns", "headers", "whitespace", "null_tokens", "booleans",
    "numbers", "dates", "case", "dedupe", "impute", "key",
)


def plan_from_config(raw: dict | None) -> CleanPlan:
    """
    Build a CleanPlan from the `cleaning:` mapping of metrics.yaml.
    None (no `cleaning:` key, or an empty one) gives the defaults.
    """
    plan = CleanPlan()
    if raw is None:
        return plan

    config = _mapping(raw, "cleaning")
    _check_keys(config, _TOP_KEYS, "cleaning")

    if "exclude_columns" in config:
        plan.exclude_columns = _names(config["exclude_columns"], "cleaning.exclude_columns")
    if "headers" in config:
        plan.headers = _bool(config["headers"], "cleaning.headers")
    if "whitespace" in config:
        plan.whitespace = _bool(config["whitespace"], "cleaning.whitespace")
    if "null_tokens" in config:
        plan.null_tokens = _null_tokens(config["null_tokens"], "cleaning.null_tokens")
    if "booleans" in config:
        plan.booleans = _bool(config["booleans"], "cleaning.booleans")
    if "numbers" in config:
        plan.numbers = _parse_options(config["numbers"], "cleaning.numbers")
    if "dates" in config:
        plan.dates = _date_options(config["dates"], "cleaning.dates")
    if "case" in config:
        plan.case = _case(config["case"], "cleaning.case")
    if "dedupe" in config:
        plan.dedupe = _dedupe(config["dedupe"], "cleaning.dedupe")
    if "impute" in config:
        plan.impute = _impute(config["impute"], "cleaning.impute")
    if "key" in config:
        plan.key_column = _name(config["key"], "cleaning.key")
    return plan


def apply_cli_overrides(
    plan:   CleanPlan,
    *,
    dedupe: str | None = None,
    key:    str | None = None,
    impute: str | None = None,
) -> CleanPlan:
    """
    Layer CLI flags over a plan. Returns a new plan; `plan` is not modified.

      --key COL        → key_column (and the dedupe key for --dedupe key)
      --dedupe exact   → drop exact duplicate rows
      --dedupe key     → dedupe on --key, else cleaning.dedupe.keys, else the plan's key
      --impute M       → impute_default (per-column YAML rules still win)
    """
    out = copy.deepcopy(plan)

    if key is not None:
        out.key_column = key

    if dedupe is not None:
        if dedupe == "exact":
            out.dedupe = DedupeOptions("exact")
        elif dedupe == "key":
            if key is not None:
                keys = [key]
            elif plan.dedupe is not None and plan.dedupe.keys:
                keys = list(plan.dedupe.keys)
            elif plan.key_column:
                keys = [plan.key_column]
            else:
                raise CleanConfigError("--dedupe key needs --key or cleaning.dedupe.keys")
            keep = plan.dedupe.keep if plan.dedupe is not None else "first"
            out.dedupe = DedupeOptions("key", keys=keys, keep=keep)
        else:
            raise CleanConfigError(f"--dedupe: expected exact or key, got {dedupe!r}")

    if impute is not None:
        if impute not in CLI_IMPUTE_CHOICES:
            raise CleanConfigError(f"--impute: expected median or mode, got {impute!r}")
        out.impute_default = impute

    return out


def load_clean_plan(
    config_path: str | Path | None = None,
    *,
    dedupe:      str | None = None,
    key:         str | None = None,
    impute:      str | None = None,
) -> CleanPlan:
    """
    Defaults < the `cleaning:` key of the config file < CLI flags.

    config_path None → ./metrics.yaml when it exists, otherwise defaults only.
    An explicit config_path must exist.
    """
    raw: dict | None = None

    if config_path is None:
        candidate = Path(DEFAULT_CONFIG_FILE)
        path = candidate if candidate.exists() else None
    else:
        path = Path(config_path)
        if not path.exists():
            raise CleanConfigError(f"{path}: config file not found")

    if path is not None:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as e:
            raise CleanConfigError(f"{path}: not UTF-8 text (byte {e.start}); save the file as UTF-8") from e
        except OSError as e:                # a directory, no permission, ...
            raise CleanConfigError(f"{path}: cannot read the config file ({e.strerror or e})") from e
        try:
            document = yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise CleanConfigError(f"{path}: invalid YAML: {e}") from e
        if document is not None and not isinstance(document, dict):
            raise CleanConfigError(f"{path}: expected a YAML mapping at the top level")
        raw = (document or {}).get("cleaning")

    plan = plan_from_config(raw)
    return apply_cli_overrides(plan, dedupe=dedupe, key=key, impute=impute)
