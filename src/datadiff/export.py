"""
export.py — Generate a self-contained HTML report (--export flag).

The output is a single .html file with no external dependencies:
all CSS is inlined, no CDN links, no JavaScript frameworks.
This means the report can be opened anywhere (email, Slack, GitHub PR).

The dark-mode terminal aesthetic is intentional: it matches the
terminal output, making the tool feel cohesive.
"""

from pathlib import Path
from datetime import datetime
from jinja2 import Template

from .differ import DiffResult


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>datadiff · {{ scenario_label }}</title>
<style>
  :root {
    --fail:    #ef4444;
    --warn:    #f59e0b;
    --info:    #3b82f6;
    --pass:    #22c55e;
    --bg:      #0f172a;
    --surface: #1e293b;
    --border:  #334155;
    --text:    #e2e8f0;
    --muted:   #94a3b8;
    --custom:  #a78bfa;   /* purple for custom metrics layer */
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
    padding: 2rem;
    max-width: 960px;
    margin: 0 auto;
  }
  h1  { font-size: 1.25rem; font-weight: 700; margin-bottom: 0.25rem; }
  .meta { color: var(--muted); font-size: 0.8rem; margin-bottom: 2rem; }

  /* ── Severity badges ─────────────────────────────────────────────────── */
  .badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.7rem; font-weight: 700; letter-spacing: 0.05em; }
  .badge-FAIL { background: #450a0a; color: var(--fail); }
  .badge-WARN { background: #451a03; color: var(--warn); }
  .badge-INFO { background: #0c1a2e; color: var(--info); }
  .badge-PASS { background: #052e16; color: var(--pass); }

  /* ── Summary stats row ───────────────────────────────────────────────── */
  .summary { display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 2rem; }
  .stat {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.875rem 1.25rem;
    min-width: 120px;
  }
  .stat-label { color: var(--muted); font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.35rem; }
  .stat-value { font-size: 1.4rem; font-weight: 700; }
  .text-fail { color: var(--fail); }
  .text-pass { color: var(--pass); }
  .text-warn { color: var(--warn); }

  /* ── Findings sections ───────────────────────────────────────────────── */
  .section       { margin-bottom: 2rem; }
  .section-title {
    color: var(--muted);
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.75rem;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid var(--border);
  }
  /* Custom metrics section gets purple accent */
  .section-title.custom { color: var(--custom); border-bottom-color: var(--custom); }

  .finding {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
    display: flex;
    gap: 0.75rem;
    align-items: flex-start;
  }
  .finding-FAIL { border-left: 3px solid var(--fail); }
  .finding-WARN { border-left: 3px solid var(--warn); }
  .finding-INFO { border-left: 3px solid var(--info); }
  .finding-PASS { border-left: 3px solid var(--pass); }

  .finding-title  { font-size: 0.875rem; line-height: 1.4; }
  .finding-detail { color: var(--muted); font-size: 0.75rem; margin-top: 0.3rem; white-space: pre-line; }

  /* ── Footer ──────────────────────────────────────────────────────────── */
  .footer { color: var(--muted); font-size: 0.75rem; margin-top: 3rem; border-top: 1px solid var(--border); padding-top: 1rem; }
</style>
</head>
<body>

<h1>datadiff report</h1>
<p class="meta">
  Generated {{ timestamp }}
  &nbsp;·&nbsp; Scenario: {{ scenario_label }}
  {% if has_custom_metrics %}
  &nbsp;·&nbsp; <span style="color: var(--custom)">Custom metrics active</span>
  {% endif %}
</p>

<!-- Summary row -->
<div class="summary">
  <div class="stat">
    <div class="stat-label">Overall</div>
    <div class="stat-value">
      <span class="badge badge-{{ result.summary_severity }}">{{ result.summary_severity }}</span>
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Rows Before</div>
    <div class="stat-value">{{ "{:,}".format(result.rows_before) }}</div>
  </div>
  <div class="stat">
    <div class="stat-label">Rows After</div>
    <div class="stat-value">{{ "{:,}".format(result.rows_after) }}</div>
  </div>
  <div class="stat">
    <div class="stat-label">Delta</div>
    <div class="stat-value {{ 'text-fail' if result.row_delta < 0 else 'text-pass' if result.row_delta > 0 else '' }}">
      {{ "{:+,}".format(result.row_delta) }}<br>
      <span style="font-size: 0.9rem">{{ "{:+.1%}".format(result.row_delta_pct) }}</span>
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Findings</div>
    <div class="stat-value">{{ result.findings | length }}</div>
  </div>
</div>

<!-- Findings by layer -->
{% for layer, label, is_custom in layers %}
{% set layer_findings = result.findings | selectattr("layer", "equalto", layer) | list %}
{% if layer_findings %}
<div class="section">
  <div class="section-title {{ 'custom' if is_custom else '' }}">{{ label }}</div>
  {% for f in layer_findings %}
  <div class="finding finding-{{ f.severity }}">
    <span class="badge badge-{{ f.severity }}">{{ f.severity }}</span>
    <div>
      <div class="finding-title">{{ f.title }}</div>
      {% if f.detail %}<div class="finding-detail">{{ f.detail }}</div>{% endif %}
    </div>
  </div>
  {% endfor %}
</div>
{% endif %}
{% endfor %}

<div class="footer">
  Generated by <strong>datadelta Δ</strong> v0.2.0
</div>
</body>
</html>
"""


def export_html(result: DiffResult, output_path: Path) -> None:
    """Render the HTML template and write to disk."""
    from .scenarios import SCENARIO_LABELS

    layers = [
        ("schema",       "Schema Changes",                      False),
        ("integrity",    "Integrity Checks",                    False),
        ("distribution", "Distribution Changes",                False),
        ("custom",       "Custom Metrics (metrics.yaml)",       True),
    ]

    has_custom_metrics = any(f.layer == "custom" for f in result.findings)

    html = Template(HTML_TEMPLATE).render(
        result             = result,
        scenario_label     = SCENARIO_LABELS.get(result.scenario, result.scenario),
        timestamp          = datetime.now().strftime("%Y-%m-%d %H:%M"),
        layers             = layers,
        has_custom_metrics = has_custom_metrics,
    )

    Path(output_path).write_text(html, encoding="utf-8")