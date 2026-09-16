"""scripts/embed_logo.py — the inline-logo constant and the asset it came from.

The renderer carries the logo as a base64 constant (pure: no file read at
render time); the PNG it was generated from is committed under assets/. This
pins the two together so a regenerated asset without a regenerated constant
(or the reverse) fails here, not in the next email.
"""
import base64
import pathlib
import sys

import pytest

from tests.conftest import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from embed_logo import constant_block  # noqa: E402

import renderer  # noqa: E402

_ASSET = REPO_ROOT / "assets" / "americhem-logo-280.png"


def test_the_renderer_constant_is_the_committed_asset():
    namespace: dict = {}
    exec(constant_block(_ASSET.read_bytes()), namespace)
    assert namespace["_LOGO_DATA_URI"] == renderer._LOGO_DATA_URI


def test_constant_block_round_trips_the_bytes_and_rejects_a_non_png():
    png = _ASSET.read_bytes()
    namespace: dict = {}
    exec(constant_block(png), namespace)
    assert base64.b64decode(namespace["_LOGO_DATA_URI"].split(",", 1)[1]) == png
    with pytest.raises(ValueError):
        constant_block(b"RIFF....WEBP")
