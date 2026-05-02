"""HTML extraction pipeline — re-exported from app.pipelines.html subpackage.

This module exists for backward compatibility. New code should import from
``app.pipelines.html`` directly.
"""

from app.pipelines.html import HtmlExtractionPipeline as HtmlExtractionPipeline

__all__ = ["HtmlExtractionPipeline"]

# ── The remainder of this file is intentionally empty. ───────────────────────
# All implementation lives in app/pipelines/html/ subpackage:
#   _css_helpers.py   — CSS colour / size conversion
#   _tag_style.py     — tag → style flag mapping + inline CSS
#   _rtl.py           — RTL direction detection
#   _state.py         — _ExtractionState dataclass
#   _run_collector.py — _RunCollector + _extract_runs helpers
#   pipeline.py       — HtmlExtractionPipeline (main class)
