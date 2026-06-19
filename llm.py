"""The LLM seam — one place every structured OpenAI call passes through.

Both engines synthesize JSON from a system+user prompt pair. Before this module
that meant three copies of the same skeleton (build client, call
``chat.completions.create`` with ``response_format=json_object``, pull
``choices[0].message.content``, ``json.loads`` it, swallow failures), each with a
slightly different empty-content guard and a different failure sentinel.

This module owns that transport and JSON-envelope handling behind a single
interface, ``LLM.complete_json``. It does **not** own response *validation* — the
caller still validates and defaults the parsed dict, because those rules are
domain-specific (relevance fields for ingestion, executive bullets for macro,
free-form paragraphs for delivery).

Two adapters sit at the seam:

- ``OpenAILLM`` — production. Owns the OpenAI client, ``OPENAI_MODEL``, and the
  json-object response format.
- ``FakeLLM`` — tests. Returns scripted dicts (or ``None``) and records every
  call so tests can assert on the system/user prompts that crossed the seam.

Callers do ``from llm import _llm`` and call ``_llm().complete_json(...)``. Tests
inject the fake at the consumer module, mirroring the repository seam:
``monkeypatch.setattr("ingestion_engine._llm", lambda: FakeLLM(returns=...))``.

Failure contract: ``complete_json`` never raises. On a transport error, empty
content, or unparseable JSON it logs and returns ``None``; each caller maps
``None`` to its own sentinel (``None`` / ``False`` / ``{}``). This preserves the
pipeline invariant that a flaky LLM downgrades a run rather than crashing it.
"""

import json
import logging
import os
from typing import Optional, Protocol

from openai import OpenAI

logger = logging.getLogger(__name__)

OPENAI_MODEL = "gpt-5.4-nano"


class LLM(Protocol):
    """Everything a caller must know to get a JSON object back from a prompt pair."""

    def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: Optional[float] = None,
        context: str = "",
    ) -> Optional[dict]:
        """Return the parsed JSON object, or None on any failure.

        ``temperature`` is omitted from the request when None (provider default).
        ``context`` is a short label used only in failure log lines.
        """
        ...


class OpenAILLM:
    """Production adapter — the only place the OpenAI client and model id live."""

    def __init__(self, model: str = OPENAI_MODEL) -> None:
        self._model = model
        self._client: Optional[OpenAI] = None

    def _get_client(self) -> OpenAI:
        if self._client is None:
            self._client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        return self._client

    def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: Optional[float] = None,
        context: str = "",
    ) -> Optional[dict]:
        suffix = f" [{context}]" if context else ""
        kwargs = {
            "model": self._model,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if temperature is not None:
            kwargs["temperature"] = temperature
        try:
            completion = self._get_client().chat.completions.create(**kwargs)
        except Exception as exc:
            logger.error("LLM call failed%s: %s", suffix, exc)
            return None
        content = completion.choices[0].message.content
        if not content:
            logger.error("LLM returned empty content%s", suffix)
            return None
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse LLM JSON response%s: %s", suffix, exc)
            return None


class FakeLLM:
    """Test adapter — scripted responses, records every call that crosses the seam.

    ``returns`` may be a single dict (returned every call), None (every call
    fails, exercising caller fallbacks), or a list consumed one-per-call
    (None once exhausted). Inspect ``calls`` to assert on the prompts a caller
    sent — each entry is ``{"system", "user", "temperature", "context"}``.
    """

    def __init__(self, returns=None) -> None:
        self._returns = returns
        self.calls: list[dict] = []

    def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: Optional[float] = None,
        context: str = "",
    ) -> Optional[dict]:
        self.calls.append(
            {"system": system, "user": user, "temperature": temperature, "context": context}
        )
        if isinstance(self._returns, list):
            return self._returns.pop(0) if self._returns else None
        return self._returns


_llm_singleton: Optional[LLM] = None


def _llm() -> LLM:
    """Return the process-wide LLM adapter (OpenAI in prod; tests inject a fake)."""
    global _llm_singleton
    if _llm_singleton is None:
        _llm_singleton = OpenAILLM()
    return _llm_singleton


def _reset_llm() -> None:
    """Drop the cached adapter — used by tests for isolation."""
    global _llm_singleton
    _llm_singleton = None
