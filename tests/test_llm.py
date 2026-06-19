"""Tests for the LLM seam (llm.py).

The SDK-shape contract — model id, json-object response format, message
ordering, and the failure-to-None envelope handling — lives here, at the
adapter, instead of being re-asserted inside every caller. Caller tests inject
``FakeLLM`` and assert on the prompts that cross the seam (see test_pipeline.py).
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from llm import OPENAI_MODEL, FakeLLM, OpenAILLM, _llm, _reset_llm


def _completion(content):
    message = MagicMock()
    message.content = content
    choice = MagicMock()
    choice.message = message
    completion = MagicMock()
    completion.choices = [choice]
    return completion


def _client_returning(content):
    client = MagicMock()
    client.chat.completions.create.return_value = _completion(content)
    return client


# ---------------------------------------------------------------------------
# OpenAILLM — request shape
# ---------------------------------------------------------------------------

def test_openai_llm_uses_model_and_json_response_format():
    client = _client_returning(json.dumps({"ok": True}))
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        adapter.complete_json(system="sys", user="usr")
    _, kwargs = client.chat.completions.create.call_args
    assert kwargs["model"] == OPENAI_MODEL
    assert kwargs["model"] == "gpt-5.4-nano"
    assert kwargs["response_format"] == {"type": "json_object"}


def test_openai_llm_passes_system_then_user_messages():
    client = _client_returning(json.dumps({"ok": True}))
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        adapter.complete_json(system="SYSTEM", user="USER")
    _, kwargs = client.chat.completions.create.call_args
    assert kwargs["messages"] == [
        {"role": "system", "content": "SYSTEM"},
        {"role": "user", "content": "USER"},
    ]


def test_openai_llm_omits_temperature_when_none():
    client = _client_returning(json.dumps({"ok": True}))
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        adapter.complete_json(system="sys", user="usr")
    _, kwargs = client.chat.completions.create.call_args
    assert "temperature" not in kwargs


def test_openai_llm_forwards_temperature_when_given():
    client = _client_returning(json.dumps({"ok": True}))
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        adapter.complete_json(system="sys", user="usr", temperature=0.2)
    _, kwargs = client.chat.completions.create.call_args
    assert kwargs["temperature"] == 0.2


# ---------------------------------------------------------------------------
# OpenAILLM — response envelope and failure contract
# ---------------------------------------------------------------------------

def test_openai_llm_parses_json_object():
    client = _client_returning(json.dumps({"headline": "X", "score": 7}))
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        result = adapter.complete_json(system="sys", user="usr")
    assert result == {"headline": "X", "score": 7}


def test_openai_llm_returns_none_on_transport_error():
    client = MagicMock()
    client.chat.completions.create.side_effect = Exception("boom")
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        result = adapter.complete_json(system="sys", user="usr")
    assert result is None


def test_openai_llm_returns_none_on_empty_content():
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=_client_returning(None)):
        assert adapter.complete_json(system="sys", user="usr") is None
    with patch.object(adapter, "_get_client", return_value=_client_returning("")):
        assert adapter.complete_json(system="sys", user="usr") is None


def test_openai_llm_returns_none_on_unparseable_json():
    client = _client_returning("not json {")
    adapter = OpenAILLM()
    with patch.object(adapter, "_get_client", return_value=client):
        result = adapter.complete_json(system="sys", user="usr")
    assert result is None


# ---------------------------------------------------------------------------
# FakeLLM — scripted responses and call recording
# ---------------------------------------------------------------------------

def test_fake_llm_returns_scripted_dict_and_records_call():
    fake = FakeLLM(returns={"a": 1})
    result = fake.complete_json(system="S", user="U", temperature=0.3, context="ctx")
    assert result == {"a": 1}
    assert fake.calls == [
        {"system": "S", "user": "U", "temperature": 0.3, "context": "ctx"}
    ]


def test_fake_llm_defaults_to_none():
    fake = FakeLLM()
    assert fake.complete_json(system="S", user="U") is None


def test_fake_llm_consumes_list_then_returns_none():
    fake = FakeLLM(returns=[{"n": 1}, {"n": 2}])
    assert fake.complete_json(system="S", user="U") == {"n": 1}
    assert fake.complete_json(system="S", user="U") == {"n": 2}
    assert fake.complete_json(system="S", user="U") is None


# ---------------------------------------------------------------------------
# Singleton seam
# ---------------------------------------------------------------------------

def test_llm_singleton_defaults_to_openai_adapter():
    _reset_llm()
    try:
        assert isinstance(_llm(), OpenAILLM)
    finally:
        _reset_llm()


def test_reset_llm_clears_cached_adapter():
    first = _llm()
    _reset_llm()
    assert _llm() is not first
    _reset_llm()
