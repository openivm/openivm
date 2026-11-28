#!/usr/bin/env python3
"""Run the refresh daemon lifetime test with a process deadline for hangs."""

import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="openivm-daemon-") as directory:
    result = subprocess.run(
        [str(Path(sys.argv[1]).resolve()), directory],
        stdout=subprocess.PIPE,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, f"daemon_lifetime_test exited with {result.returncode}"
    assert result.stdout.strip() == "PASS", result.stdout
print("Refresh daemons are per database and stop with their database")
