"""Emit the renderer's inline-logo constant from a PNG — an operator script,
not part of the cron.

The digest carries its logo as a data URI (`renderer._LOGO_DATA_URI`) rather
than loading it from www.americhem.com: Resend's Insights flagged a
brand-domain image on a non-brand sender as an impersonation signal after
the 2026-09-16 Proofpoint quarantine. The renderer is pure — it holds the
image as a module-level constant and never reads a file — so a new logo is
a paste of this script's output into renderer.py.

Keep the image small: it ships in every email, twice (header and footer),
and Gmail clips a message above ~102 KB. The shipped file,
assets/americhem-logo-280.png, is the site's 700x136 WebP resized to 280 px
wide (2x the 140 px the header renders it at) and quantized to a 64-colour
palette PNG (Pillow: `im.resize((280, 54), Image.LANCZOS)
.quantize(colors=64).save(..., optimize=True)`): 4.2 KB on disk, 5.6 KB as
base64. PNG, not WebP, because Outlook's Word engine renders no WebP.

Usage:
    python scripts/embed_logo.py assets/americhem-logo-280.png > /tmp/logo.txt
then replace the `_LOGO_DATA_URI = (...)` block in renderer.py with the output.
"""
import base64
import sys
from pathlib import Path

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_WRAP = 88


def constant_block(png: bytes) -> str:
    """The Python source of `_LOGO_DATA_URI` for `png`: a parenthesized
    implicit concatenation of `_WRAP`-character chunks."""
    if png[:8] != _PNG_SIGNATURE:
        raise ValueError("not a PNG")
    b64 = base64.b64encode(png).decode("ascii")
    chunks = [b64[i:i + _WRAP] for i in range(0, len(b64), _WRAP)]
    lines = ['_LOGO_DATA_URI = (', '    "data:image/png;base64,"']
    lines += [f'    "{chunk}"' for chunk in chunks]
    lines.append(")")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: embed_logo.py <logo.png>", file=sys.stderr)
        return 2
    print(constant_block(Path(argv[1]).read_bytes()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
