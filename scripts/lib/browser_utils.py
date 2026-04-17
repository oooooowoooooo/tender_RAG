import shutil
import subprocess
from pathlib import Path

from .io_utils import ROOT


def find_chrome_executable() -> str | None:
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
    return shutil.which("chrome") or shutil.which("msedge")


def dump_dom_with_chrome(url: str, output_path: Path, timeout_seconds: int = 60) -> tuple[bool, str]:
    chrome = find_chrome_executable()
    if not chrome:
        return False, "browser_unavailable"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    user_data_dir = ROOT / "data" / "staging" / "browser_profile"
    user_data_dir.mkdir(parents=True, exist_ok=True)
    command = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--disable-breakpad",
        "--disable-crash-reporter",
        "--no-first-run",
        "--no-default-browser-check",
        "--virtual-time-budget=10000",
        f"--user-data-dir={user_data_dir}",
        "--dump-dom",
        url,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"

    stdout_text = result.stdout.decode("utf-8", errors="ignore") if isinstance(result.stdout, bytes) else str(result.stdout or "")
    stderr_text = result.stderr.decode("utf-8", errors="ignore") if isinstance(result.stderr, bytes) else str(result.stderr or "")
    if stdout_text:
        output_path.write_text(stdout_text, encoding="utf-8")
    if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return True, "success"
    return False, (stderr_text or stdout_text or "browser_failed").strip()
