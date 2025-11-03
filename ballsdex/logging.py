import logging
import logging.handlers
from pathlib import Path
from queue import Queue

from discord.utils import _ColourFormatter

log = logging.getLogger("ballsdex")


def init_logger(disable_rich: bool = False, debug: bool = False) -> logging.handlers.QueueListener:
    formatter = logging.Formatter(
        "[{asctime}] {levelname} {name}: {message}", datefmt="%Y-%m-%d %H:%M:%S", style="{"
    )
    rich_formatter = _ColourFormatter()

    # handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    stream_handler.setFormatter(formatter if disable_rich else rich_formatter)

    # file handler - try multiple locations in case of permission issues
    file_handler = None
    log_paths = [
        "ballsdex.log",  # Try current directory first
        Path("logs") / "ballsdex.log",  # Try logs subdirectory
        Path("/tmp") / "ballsdex.log",  # Fallback to /tmp
    ]

    for log_path in log_paths:
        try:
            # Ensure directory exists if using a subdirectory
            if isinstance(log_path, Path):
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path = str(log_path)

            file_handler = logging.handlers.RotatingFileHandler(
                log_path, maxBytes=8**7, backupCount=8
            )
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            # Note: Logging here might not work yet, but handler creation succeeded
            break
        except (PermissionError, OSError):
            # Continue to next path if this one fails
            continue

    if file_handler is None:
        # Note: Logger not fully configured yet, but this will be logged after setup
        pass

    queue = Queue(-1)
    queue_handler = logging.handlers.QueueHandler(queue)

    root = logging.getLogger()
    root.addHandler(queue_handler)
    root.setLevel(logging.INFO)
    log.setLevel(logging.DEBUG if debug else logging.INFO)

    # Only add file_handler to queue_listener if it was successfully created
    if file_handler is not None:
        queue_listener = logging.handlers.QueueListener(queue, stream_handler, file_handler)
    else:
        queue_listener = logging.handlers.QueueListener(queue, stream_handler)
    queue_listener.start()

    logging.getLogger("aiohttp").setLevel(logging.WARNING)  # don't log each prometheus call

    # Log where the file handler was created (if successful)
    if file_handler is not None:
        log.info(f"File logging enabled at: {file_handler.baseFilename}")
    else:
        log.warning("File logging disabled due to permission errors")

    return queue_listener
