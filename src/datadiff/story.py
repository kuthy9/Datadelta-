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

PRIVACY:
  We only send the structured DiffResult (findings as JSON) to the API.
  Raw data rows never leave your machine, even for million-row datasets.
"""

from __future__ import annotations

import os
import json
from typing import TYPE_CHECKING, Literal
from rich.console import Console

from .differ import DiffResult

if TYPE_CHECKING:
    from .metrics import MetricsConfig

console = Console()

# ── Supported providers ───────────────────────────────────────────────────────
LLMProvider = Literal["claude", "openai", "deepseek", "gemini"]

PROVIDER_CONFIG = {
    "claude": {
        "env_key":   "ANTHROPIC_API_KEY",
        "model":     "claude-sonnet-4-20250514",
        "label":     "Claude (Anthropic)",
    },
    "openai": {
        "env_key":   "OPENAI_API_KEY",
        "model":     "gpt-4o-mini",
        "label":     "GPT-4o mini (OpenAI)",
    },
    "deepseek": {
        "env_key":   "DEEPSEEK_API_KEY",
        "model":     "deepseek-chat",
        "label":     "DeepSeek Chat",
        "base_url":  "https://api.deepseek.com",
    },
    "gemini": {
        "env_key":   "GEMINI_API_KEY",
        "model":     "gemini-1.5-flash",
        "label":     "Gemini 1.5 Flash (Google)",
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

def generate_story(
    result:         DiffResult,
    metrics_config: "MetricsConfig | None" = None,
    provider:       LLMProvider = "claude",
) -> None:
    """
    Generate a natural language narrative and stream it to the terminal.
    Dispatches to the appropriate provider handler.
    """
    config = PROVIDER_CONFIG.get(provider)
    if not config:
        console.print(f"[red]Unknown LLM provider: '{provider}'[/red]")
        console.print(f"[dim]Supported: {', '.join(PROVIDER_CONFIG.keys())}[/dim]")
        return

    # Check API key
    api_key = os.environ.get(config["env_key"])
    if not api_key:
        console.print(f"\n[yellow]Warning: {config['env_key']} not set. Skipping --story.[/yellow]")
        console.print(f"[dim]  export {config['env_key']}=your_key[/dim]\n")
        return

    # Build prompts
    system_prompt = _build_system_prompt(metrics_config)
    user_message  = _build_user_message(result)

    console.print(f"\n[bold]Story[/bold]  [dim]via {config['label']}[/dim]\n")

    # Dispatch to the right provider
    if provider == "claude":
        _call_claude(api_key, config["model"], system_prompt, user_message)
    elif provider in ("openai", "deepseek"):
        _call_openai_compatible(
            api_key, config["model"], system_prompt, user_message,
            base_url=config.get("base_url"),
        )
    elif provider == "gemini":
        _call_gemini(api_key, config["model"], system_prompt, user_message)

    console.print("\n")


# ─────────────────────────────────────────────────────────────────────────────
# Provider implementations
# ─────────────────────────────────────────────────────────────────────────────

def _call_claude(api_key: str, model: str, system: str, user: str) -> None:
    """
    Anthropic Claude API.
    Uses the native `anthropic` library with streaming.
    The `with client.messages.stream(...)` context manager yields
    text chunks as they arrive from the server.
    """
    try:
        import anthropic
    except ImportError:
        console.print("[red]Install the Anthropic library: pip install anthropic[/red]")
        return

    client = anthropic.Anthropic(api_key=api_key)

    with client.messages.stream(
        model      = model,
        max_tokens = 500,
        system     = system,
        messages   = [{"role": "user", "content": user}],
    ) as stream:
        for text in stream.text_stream:
            console.print(text, end="")


def _call_openai_compatible(
    api_key:  str,
    model:    str,
    system:   str,
    user:     str,
    base_url: str | None = None,
) -> None:
    """
    OpenAI-compatible API. Works for both OpenAI and DeepSeek.

    DeepSeek's API mirrors the OpenAI interface exactly — same request/response
    format, same streaming protocol. The only difference is the base_url and
    the model name. So we reuse the `openai` library for both, just pointing
    it at a different server via base_url.

    stream=True means the response comes back as a generator.
    Each chunk has: chunk.choices[0].delta.content (the next text piece, or None).
    """
    try:
        from openai import OpenAI
    except ImportError:
        console.print("[red]Install the OpenAI library: pip install openai[/red]")
        return

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

    for chunk in response:
        text = chunk.choices[0].delta.content
        if text:
            console.print(text, end="")


def _call_gemini(api_key: str, model: str, system: str, user: str) -> None:
    """
    Google Gemini API via the `google-generativeai` library.

    Gemini handles the system prompt differently from OpenAI/Anthropic:
    it's passed as `system_instruction` at model initialization time,
    not as a message in the conversation.

    generate_content(..., stream=True) returns a generator of response chunks.
    Each chunk has a `.text` attribute with the next piece of text.
    """
    try:
        import google.generativeai as genai
    except ImportError:
        console.print(
            "[red]Install the Google AI library: pip install google-generativeai[/red]"
        )
        return

    genai.configure(api_key=api_key)

    # system_instruction is Gemini's equivalent of a system prompt
    gemini_model = genai.GenerativeModel(
        model_name         = model,
        system_instruction = system,
    )

    response = gemini_model.generate_content(user, stream=True)

    for chunk in response:
        if chunk.text:
            console.print(chunk.text, end="")


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
    """Serialize the DiffResult as JSON for the LLM user message."""
    payload = json.dumps(result.to_dict(), indent=2)
    return (
        f"Here is the diff result:\n\n"
        f"```json\n{payload}\n```\n\n"
        f"Write the narrative summary now."
    )