import os
import sys

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
