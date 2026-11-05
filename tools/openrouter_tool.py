"""Compatibility shim: the OpenRouter client grew into a multi-provider tool (see llm_tool.py)."""
from tools.llm_tool import LLMError, LLMResult, LLMTool, OpenRouterTool, extract_json  # noqa: F401
