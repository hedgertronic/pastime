"""Download files with concurrency and progress bars."""

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


# Rich console for printing
CONSOLE = Console()


# Generic task description template to be filled in by progress bar
_TASK_DESCRIPTION = "[bold #AAAAAA]({done} out of {total} files downloaded)"


def _current_request_progress() -> Progress:
    """Return progress bar that represents the current request."""
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
    """Return progress bar that represents overall progress of all requests."""
    return Progress(TimeElapsedColumn(), BarColumn(), TextColumn("{task.description}"))


def _progress_group(current_app_progress, overall_progress) -> Group:
    """Return group of current progress bars with overall progress bar."""
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
    """Download a single file.

    Args:
        url (str): The URL to request from.
        params (dict[str, list[str]]): A dict of params to include with the request.
        request_name (str | None, optional): The name of the request. Defaults to None.
        messages (Sequence[str], optional): Messages to display before the request.
            Defaults to None.

    Returns:
        io.StringIO: A String IO object that contains the data from the requests.
    """
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
    """Download multiple files from a URL with multiple combinations of parameters.

    Args:
        url (str): The URL to request from.
        params (Sequence[dict[str, list[str]]]): A sequence of param dicts with each
            one representing a different request to make.
        request_name (str | None, optional): The name of the request. Defaults to None.
        messages (Sequence[str], optional): Messages to display before the request.
            Defaults to None.

    Returns:
        io.StringIO: A String IO object that contains the data from all requests.
    """
    output = io.StringIO()

    if request_name:
        CONSOLE.rule(f"[bold blue]{request_name}")

    message_printed = False

    if len(params) > 1:
        CONSOLE.print(
            f"There are {len(params)} requests to make. This may take a while.",
        )

        message_printed = True

    if messages:
        for message in messages:
            if message_printed:
                CONSOLE.print()
            else:
                message_printed = True

            CONSOLE.print(message)

    ###################################################################################
    # PROGRESS BAR SETUP -- DIFFERENT INSTANCE EVERY TIME

    current_app_progress = _current_request_progress()
    overall_progress = _overall_progress()

    tasks_complete = 0

    overall_task_id = overall_progress.add_task(
        _TASK_DESCRIPTION.format(done=tasks_complete, total=len(params)),
        total=len(params),
    )

    ###################################################################################
    # INNER FUNCTION -- NEEDS ACCESS TO PROGRESS BAR VARIABLES

    # Making the function that is sent to the thread pool an inner function was the
    # simplest way to give it access to all progress bar and task variables. There
    # probably exists a nicer way of implementing this somewhere down the line.

    def make_request(
        url: str,
        output: IO,
        **kwargs,
    ) -> IO:
        """Make a request to a given URL and return the output.

        Args:
            url (str): The URL to request from.
            output (IO): The IO object to write to.

        Returns:
            IO: The IO object that was written to.
        """
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

        # This is necessary to access the variable from the outer scope. This may not
        # be the cleanest way of handling this, but passing the variable into the
        # function did not work because of the concurrency.

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

            # Wait a second between requests so we don't piss off whoever is nice
            # enough to host the data we are accessing :)

            time.sleep(1)

    return output
