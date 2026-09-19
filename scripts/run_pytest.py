from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JAVA_HOME = Path(r"C:\Program Files\Java\jdk-17")
OUT = ROOT / "test_results.txt"


def main() -> int:
    env = os.environ.copy()
    if JAVA_HOME.exists():
        env["JAVA_HOME"] = str(JAVA_HOME)
        env["PATH"] = str(JAVA_HOME / "bin") + os.pathsep + env.get("PATH", "")
    python = ROOT / ".venv" / "Scripts" / "python.exe"
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [str(python), "-m", "pytest", "-vv", "--tb=long", "-p", "no:cacheprovider"]
    with OUT.open("w", encoding="utf-8") as handle:
        proc = subprocess.run(cmd, cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT, text=True)
    print(OUT.read_text(encoding="utf-8"))
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
