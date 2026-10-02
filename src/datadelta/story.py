"""
story.py — Natural language narrative via LLM API (--story flag).

Supports four LLM providers:
  claude    → Anthropic API  (requires ANTHROPIC_API_KEY)
  openai    → OpenAI API     (requires OPENAI_API_KEY)
  deepseek  → DeepSeek API   (requires DEEPSEEK_API_KEY)
             Note: DeepSeek's API is OpenAI-compatible, so we reuse
             the openai library pointed at a different base_url.
  gemini    → Google Gemini  (requires GEMINI_API_KEY)

WHY THESE FOUR?
  - Claude:   best at nuanced narrative, native streaming support
  - OpenAI:   most widely used, many users already have a key
  - DeepSeek: very cheap, strong reasoning, OpenAI-compatible interface
  - Gemini:   Google ecosystem users, free tier available

HOW THE "SKILL" INJECTION WORKS:
  If metrics.yaml has a business_context block, it is appended to the
  system prompt before calling the LLM. This makes the narrative use
  your company's vocabulary. This works the same way across all providers
  because all of them support a system prompt / system message.

BUFFERED OUTPUT:
  generate_story() collects the complete narrative and returns it as a
  StoryResult (text + provider label); it never prints the narrative.
  The CLI prints it after the report with reporter.print_story(), wrapped
  to the terminal width, so streamed chunks can never interleave with the
  report on stdout or the progress display on stderr. Warnings (missing
  API key, missing SDK, provider errors) go to stderr, and a failed story
  returns None instead of failing the diff.

PRIVACY:
  Raw data rows never leave your machine. The LLM receives story_payload():
  the DiffResult as JSON — column names, aggregate statistics, finding
  titles — minus every primary-key sample value, with each finding's
  category-label list capped at 10 labels ("+k more" for the rest), and with
  metric-error findings reduced to the metric name and exception type.
  With --clean, the cleaning reports are reduced to their totals (cells
  changed, rows dropped, retyped columns, number of actions); the
  before → after examples never leave your machine.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing      import TYPE_CHECKING, Literal

from rich.console import Console
from rich.markup  import escape

from .             import theme
from .clean.report import CleanAction, CleanReport, CleanStats
from .differ       import DiffResult
from .jsonutil     import dumps
from .safetext     import printable

if TYPE_CHECKING:
    from .metrics import MetricsConfig

# ── Supported providers ───────────────────────────────────────────────────────
LLMProvider = Literal["claude", "openai", "deepseek", "gemini"]

PROVIDER_CONFIG = {
    "claude": {
        "env_key":   "ANTHROPIC_API_KEY",
        "model":     "claude-sonnet-4-20250514",
        "label":     "Claude (Anthropic)",
        "install":   "pip install anthropic",
    },
    "openai": {
        "env_key":   "OPENAI_API_KEY",
        "model":     "gpt-4o-mini",
        "label":     "GPT-4o mini (OpenAI)",
        "install":   'pip install "datadelta-cli[openai]"',
    },
    "deepseek": {
        "env_key":   "DEEPSEEK_API_KEY",
        "model":     "deepseek-chat",
        "label":     "DeepSeek Chat",
        "base_url":  "https://api.deepseek.com",
        "install":   'pip install "datadelta-cli[openai]"',
    },
    "gemini": {
        "env_key":   "GEMINI_API_KEY",
        "model":     "gemini-1.5-flash",
        "label":     "Gemini 1.5 Flash (Google)",
        "install":   'pip install "datadelta-cli[gemini]"',
    },
}

# ── Prompts ───────────────────────────────────────────────────────────────────

BASE_SYSTEM_PROMPT = """You are a senior data analyst writing a change summary for a technical audience.

You will receive a structured diff result between two versions of a dataset.
Write a concise narrative (3-5 sentences) that:
1. Summarizes the most important changes (focus on FAIL and WARN findings)
2. Notes anything suspicious or worth investigating
3. Uses plain, direct language - no fluff, no bullet points, no headers

Do not repeat every finding verbatim. Tell the story: what changed, what
stands out, what should someone investigate first. If custom metrics are
present in the findings, prioritize them as they represent business-critical KPIs.
"""

CONTEXT_BLOCK_TEMPLATE = """
---
Business context for this dataset:
{context}

Use the above context to interpret the findings in business terms.
Refer to metrics by their business names (not column names) where possible.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StoryResult:
    """A finished narrative and the label of the provider that wrote it."""
    text:  str
    label: str      # e.g. "Claude (Anthropic)" — shown in the story header


def generate_story(
    result:         DiffResult,
    metrics_config: "MetricsConfig | None" = None,
    provider:       LLMProvider = "claude",
    *,
    console:        Console | None = None,
) -> StoryResult | None:
    """
    Ask the provider for a narrative and return it in full.

    Returns None (after a warning on stderr) when the provider is unknown,
    its API key is not set, its SDK is not installed, the call raises, or
    the reply is empty. Never prints the narrative and never raises for a
    provider error: a missing story must not fail the diff.

    console: where the warnings go (default: a new stderr Console). The CLI
    passes the console its live progress display runs on, so a warning is
    printed above the display instead of through it (under --quiet, a
    Console(stderr=True, quiet=True) that prints nothing).
    """
    err = console if console is not None else Console(stderr=True)

    config = PROVIDER_CONFIG.get(provider)
    if not config:
        err.print(f"[{theme.ERROR_STYLE}]Unknown LLM provider:[/] {escape(repr(provider))}")
        err.print(f"[dim]Supported: {', '.join(PROVIDER_CONFIG.keys())}[/dim]")
        return None

    # Check API key
    api_key = os.environ.get(config["env_key"])
    if not api_key:
        err.print(f"[{theme.WARNING_STYLE}]Warning: {config['env_key']} not set. Skipping --story.[/]")
        err.print(f"[dim]  export {config['env_key']}=your_key[/dim]")
        return None

    # Build prompts
    system_prompt = _build_system_prompt(metrics_config)
    user_message  = _build_user_message(result)

    # Dispatch to the right provider; collect the whole reply
    try:
        if provider == "claude":
            text = _call_claude(api_key, config["model"], system_prompt, user_message)
        elif provider in ("openai", "deepseek"):
            text = _call_openai_compatible(
                api_key, config["model"], system_prompt, user_message,
                base_url=config.get("base_url"),
            )
        else:
            text = _call_gemini(api_key, config["model"], system_prompt, user_message)
    except ImportError:
        err.print(
            f"[{theme.WARNING_STYLE}]story skipped:[/] the SDK for {escape(config['label'])} "
            f"is not installed. Install it: {escape(config['install'])}",
            soft_wrap = True,
        )
        return None
    except Exception as e:  # any SDK / network / quota error: warn, keep the report
        err.print(
            f"[{theme.WARNING_STYLE}]story failed:[/] {escape(type(e).__name__)}: {escape(printable(e))}",
            soft_wrap = True,
        )
        return None

    text = text.strip()
    if not text:
        err.print(f"[{theme.WARNING_STYLE}]story failed:[/] the LLM returned no text", soft_wrap=True)
        return None
    return StoryResult(text=text, label=config["label"])


# ─────────────────────────────────────────────────────────────────────────────
# Provider implementations — each returns the complete reply text.
# A missing SDK raises ImportError; generate_story() turns it into a warning.
# ─────────────────────────────────────────────────────────────────────────────

def _call_claude(api_key: str, model: str, system: str, user: str) -> str:
    """
    Anthropic Claude API.
    Uses the native `anthropic` library with streaming.
    The `with client.messages.stream(...)` context manager yields
    text chunks as they arrive from the server; we join them.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    chunks: list[str] = []
    with client.messages.stream(
        model      = model,
        max_tokens = 500,
        system     = system,
        messages   = [{"role": "user", "content": user}],
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
    return "".join(chunks)


def _call_openai_compatible(
    api_key:  str,
    model:    str,
    system:   str,
    user:     str,
    base_url: str | None = None,
) -> str:
    """
    OpenAI-compatible API. Works for both OpenAI and DeepSeek.

    DeepSeek's API mirrors the OpenAI interface exactly — same request/response
    format, same streaming protocol. The only difference is the base_url and
    the model name. So we reuse the `openai` library for both, just pointing
    it at a different server via base_url.

    stream=True means the response comes back as a generator.
    Each chunk has: chunk.choices[0].delta.content (the next text piece, or None).
    """
    from openai import OpenAI

    # base_url=None  → uses default OpenAI endpoint (api.openai.com)
    # base_url=...   → routes to DeepSeek or any other OpenAI-compatible server
    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model      = model,
        stream     = True,
        max_tokens = 500,
        messages   = [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
    )

    chunks: list[str] = []
    for chunk in response:
        if not chunk.choices:          # some servers send a final usage-only chunk
            continue
        text = chunk.choices[0].delta.content
        if text:
            chunks.append(text)
    return "".join(chunks)


def _call_gemini(api_key: str, model: str, system: str, user: str) -> str:
    """
    Google Gemini API via the `google-generativeai` library.

    Gemini handles the system prompt differently from OpenAI/Anthropic:
    it's passed as `system_instruction` at model initialization time,
    not as a message in the conversation.

    generate_content(..., stream=True) returns a generator of response chunks.
    Each chunk has a `.text` attribute with the next piece of text.
    """
    import google.generativeai as genai

    genai.configure(api_key=api_key)

    # system_instruction is Gemini's equivalent of a system prompt
    gemini_model = genai.GenerativeModel(
        model_name         = model,
        system_instruction = system,
    )

    response = gemini_model.generate_content(user, stream=True)

    chunks: list[str] = []
    for chunk in response:
        if chunk.text:
            chunks.append(chunk.text)
    return "".join(chunks)


# ─────────────────────────────────────────────────────────────────────────────
# Prompt builders (shared across all providers)
# ─────────────────────────────────────────────────────────────────────────────

def _build_system_prompt(metrics_config: "MetricsConfig | None") -> str:
    """
    Build the system prompt, optionally injecting business_context
    from metrics.yaml. This is the 'skill injection' mechanism —
    the LLM now speaks your company's language.
    """
    prompt = BASE_SYSTEM_PROMPT
    if metrics_config and metrics_config.business_context.strip():
        prompt += CONTEXT_BLOCK_TEMPLATE.format(
            context=metrics_config.business_context.strip()
        )
    return prompt


def _build_user_message(result: DiffResult) -> str:
    """Serialize the privacy-reduced story payload as JSON for the LLM user message."""
    payload = dumps(story_payload(result))
    return (
        f"Here is the diff result:\n\n"
        f"```json\n{payload}\n```\n\n"
        f"Write the narrative summary now."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Story payload — what the LLM is allowed to see
# ─────────────────────────────────────────────────────────────────────────────

# Category-label lists carried by distribution findings, and the title
# differ.py builds around each list (rebuilt here with the capped list).
_LABEL_TITLES = {
    "new_values":     "New categories in '{column}': {labels}",
    "missing_values": "Categories disappeared from '{column}': {labels}",
}


def story_payload(result: DiffResult, max_labels: int = 10) -> dict:
    """
    The DiffResult as the LLM sees it: result.to_dict() with
      - every "sample" list (primary-key values) removed from the finding's
        metric, and the detail text that quoted it rewritten without it;
      - every new/missing category list sorted and capped at `max_labels`
        labels, the rest summarized as one "+k more" entry, and the title
        rebuilt from the capped list;
      - every metric-error finding (a metrics.yaml expression that raised)
        reduced to "[metrics.yaml error] '<name>': <ExceptionType>" with a
        fixed detail sentence, because the exception text can quote a cell;
      - every category finding on the key column (result.key_column: a
        non-unique --key such as an email profiles as a category) reduced
        to counts: no label list, no share-shift value, value-free titles;
      - with --clean, "cleaning" reduced to {"before": counts, "after":
        counts} (CleanReport.summary_counts(): no examples, no notes).
    Counts, rates and all other fields are unchanged. The result object
    itself is not modified.
    """
    payload = result.to_dict()
    payload["findings"] = [
        _reduce_finding(_without_key_values(f) if _on_key(f, result.key_column) else f, max_labels)
        for f in payload["findings"]
    ]
    if "cleaning" in payload:
        payload["cleaning"] = {side: _cleaning_counts(report) for side, report in payload["cleaning"].items()}
    return payload


def _cleaning_counts(report: dict) -> dict:
    """CleanReport.summary_counts() of a serialized report: totals only, no examples."""
    return CleanReport(
        actions = [CleanAction(**{k: v for k, v in a.items() if k != "examples"}) for a in report["actions"]],
        stats   = CleanStats(**report["stats"]),
    ).summary_counts()


def _reduce_finding(finding: dict, max_labels: int) -> dict:
    """Return a privacy-reduced copy of one serialized finding."""
    finding = dict(finding)
    metric  = dict(finding["metric"])   # to_dict() shares the metric dict with the Finding

    if "sample" in metric:
        del metric["sample"]
        finding["detail"] = _detail_without_sample(finding["column"], metric)

    for key, template in _LABEL_TITLES.items():
        if key in metric:
            metric[key]      = _cap_labels(metric[key], max_labels)
            finding["title"] = template.format(column=finding["column"], labels=metric[key])

    if metric.get("error"):
        # The exception text may quote a cell value; keep name and type only.
        finding["title"]  = f"[metrics.yaml error] '{metric['name']}': {metric['error_type']}"
        finding["detail"] = f"Check your metrics.yaml definition for '{metric['name']}'."

    finding["metric"] = metric
    return finding


def _on_key(finding: dict, key_column: str | None) -> bool:
    return key_column is not None and finding["layer"] == "distribution" and finding["column"] == key_column


def _without_key_values(finding: dict) -> dict:
    """
    A distribution finding on the key column with every key value replaced
    by a count: new/missing category lists become new_value_count /
    missing_value_count, a share shift loses its value; titles and details
    are rebuilt without values. Rates and shares are kept.
    """
    finding = dict(finding)
    metric  = dict(finding["metric"])
    column  = finding["column"]

    if "new_values" in metric:
        count = len(metric.pop("new_values"))
        metric["new_value_count"] = count
        finding["title"]  = f"New categories in '{column}': {count:,} key value(s) withheld"
        finding["detail"] = f"{count:,} new value(s) appeared in key column '{column}'."
    if "missing_values" in metric:
        count = len(metric.pop("missing_values"))
        metric["missing_value_count"] = count
        finding["title"]  = f"Categories disappeared from '{column}': {count:,} key value(s) withheld"
    if "value" in metric and "delta" in metric:
        del metric["value"]
        direction = "grew" if metric["delta"] > 0 else "shrank"
        finding["title"]  = (
            f"'{column}' share of one key value {direction}: "
            f"{metric['before']:.1%} → {metric['after']:.1%} ({metric['delta']:+.1%})"
        )
        finding["detail"] = f"The proportion of one key value in column '{column}' changed by {metric['delta']:+.1%}."

    finding["metric"] = metric
    return finding


def _detail_without_sample(column: str | None, metric: dict) -> str:
    """differ.py quotes key samples in integrity details; restate them without values."""
    if "duplicate_count" in metric:
        return (
            f"Column '{column}' has {metric['duplicate_count']:,} duplicate value(s) "
            f"in the after dataset."
        )
    if "deleted_count" in metric:
        return "Keys present in before but not in after."
    if "added_count" in metric:
        return "New keys in after that weren't in before."
    return "Sample values withheld from the LLM."


def _cap_labels(labels: list, max_labels: int) -> list[str]:
    """Sorted labels, at most `max_labels`, plus "+k more" when some were cut."""
    ordered = sorted(str(v) for v in labels)
    if len(ordered) <= max_labels:
        return ordered
    return ordered[:max_labels] + [f"+{len(ordered) - max_labels} more"]
