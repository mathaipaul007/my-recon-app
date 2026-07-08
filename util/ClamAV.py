import asyncio
import os
import tempfile
from pathlib import Path

class VirusDetectedException(Exception):
    pass

class VirusScanException(Exception):
    pass

async def scan_file_with_clamscan(file_path: str) -> None:
    """
    Runs clamscan against the given file.

    clamscan exit codes:
    0 = clean
    1 = infected
    2 = error
    """
    print("scanning the file", file_path)
    process = await asyncio.create_subprocess_exec(
        "clamscan",
        "--no-summary",
        file_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    stdout_text = stdout.decode(errors="ignore")
    stderr_text = stderr.decode(errors="ignore")

    if process.returncode == 0:
        return

    if process.returncode == 1:
        raise VirusDetectedException(stdout_text)

    raise VirusScanException(
        f"ClamAV scan failed. stdout={stdout_text}, stderr={stderr_text}"
)

