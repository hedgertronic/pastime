from contextlib import closing
from typing import IO

import requests
from rich.progress import Progress


def download_csv(url: str, output: IO, progress_bar: Progress, **kwargs):
    task_id = progress_bar.add_task(
        description="Making request...",
        total=None,
    )

    with closing(requests.get(url=url, timeout=180, stream=True, **kwargs)) as response:
        print(response.headers)
        total_length = int(response.headers["Content-Length"])

        progress_bar.update(task_id, total=total_length)

        for line in response.iter_lines(decode_unicode=True):
            output.write(line + "\n")
            progress_bar.update(task_id, advance=len(line))

        progress_bar.update(task_id, completed=total_length)

    return output
