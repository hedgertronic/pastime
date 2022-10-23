import io
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import IO, Any, Sequence

import requests
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)


#######################################################################################
# PROGRESS BAR AND CONSOLE


console = Console()


_TASK_DESCRIPTION = "[bold #AAAAAA]({done} out of {total} files downloaded)"


def _current_app_progress() -> Progress:
    return Progress(
        TextColumn("{task.description}"),
        "[progress.percentage]{task.percentage:>3.1f}%",
        BarColumn(),
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeElapsedColumn(),
    )


def _overall_progress() -> Progress:
    return Progress(TimeElapsedColumn(), BarColumn(), TextColumn("{task.description}"))


def _progress_group(current_app_progress, overall_progress) -> Group:
    return Group(
        Panel(Group(current_app_progress)),
        overall_progress,
    )


#######################################################################################
# DOWNLOAD FUNCTIONS


def download_file(
    url: str,
    params: dict[str, list[str]],
    request_name: str | None = None,
    messages: Sequence[str] = None,
    **kwargs: Any,
) -> io.StringIO:
    return download_files(
        url,
        [params],
        request_name=request_name,
        messages=messages,
        **kwargs,
    )


def download_files(
    url: str,
    params: Sequence[dict[str, list[str]]],
    request_name: str | None = None,
    messages: Sequence[str] = None,
    **kwargs: Any,
) -> io.StringIO:
    output = io.StringIO()

    if len(params) > 1:
        console.print(
            f"There are {len(params)} requests to make. This may take a while.",
        )

    if request_name:
        console.rule(f"[bold blue]{request_name}")

    if messages:
        for message in messages:
            console.print(message)

    ###################################################################################
    # PROGRESS BAR SETUP -- DIFFERENT INSTANCE EVERY TIME

    current_app_progress = _current_app_progress()
    overall_progress = _overall_progress()

    tasks_complete = 0

    overall_task_id = overall_progress.add_task(
        _TASK_DESCRIPTION.format(done=tasks_complete, total=len(params)),
        total=len(params),
    )

    ###################################################################################
    # INNER FUNCTION -- NEEDS ACCESS TO PROGRESS BAR VARIABLES

    def make_request(
        url: str,
        output: IO,
        **kwargs,
    ):
        task_id = current_app_progress.add_task(
            description="Making request...",
            total=None,
        )

        with closing(
            requests.get(url=url, timeout=180, stream=True, **kwargs)
        ) as response:
            response.raise_for_status()

            total_length = int(response.headers.get("Content-Length", 100_000))

            current_app_progress.update(task_id, total=total_length)

            for line in response.iter_lines(decode_unicode=True):
                output.write(line + "\n")
                current_app_progress.update(task_id, advance=len(line))

            current_app_progress.update(task_id, completed=total_length)

        current_app_progress.stop_task(task_id)
        current_app_progress.update(task_id, description="[bold green]File downloaded!")

        nonlocal tasks_complete
        tasks_complete += 1

        if tasks_complete != len(params):
            update_string = _TASK_DESCRIPTION.format(
                done=tasks_complete, total=len(params)
            )

        else:
            update_string = (
                f"[bold green]{len(params)}"
                f" file{'s' if len(params) > 1 else ''} downloaded, done!"
            )

        overall_progress.update(overall_task_id, advance=1, description=update_string)

        return output

    ###################################################################################
    # MANAGING MULTIPLE CONCURRENT REQUESTS

    with Live(
        _progress_group(current_app_progress, overall_progress)
    ), ThreadPoolExecutor() as pool:
        for param in params:
            pool.submit(
                make_request,
                url,
                output,
                params=param,
                **kwargs,
            )

            time.sleep(1)

    console.print("\n")

    return output
