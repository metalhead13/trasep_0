from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JAVA_HOME = Path(r"C:\Program Files\Java\jdk-17")
OUT = ROOT / "batch_results.txt"


def main() -> int:
    env = os.environ.copy()
    if JAVA_HOME.exists():
        env["JAVA_HOME"] = str(JAVA_HOME)
        env["PATH"] = str(JAVA_HOME / "bin") + os.pathsep + env.get("PATH", "")
    python = ROOT / ".venv" / "Scripts" / "python.exe"
    with OUT.open("w", encoding="utf-8") as handle:
        for args in (["-m", "trasep", "seed"], ["-m", "trasep", "batch"]):
            handle.write(f"\n>>> python {' '.join(args)}\n")
            handle.flush()
            proc = subprocess.run(
                [str(python), *args],
                cwd=ROOT,
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
            if proc.returncode != 0:
                handle.write(f"\nexit {proc.returncode}\n")
                print(OUT.read_text(encoding="utf-8"))
                return proc.returncode
    print(OUT.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
