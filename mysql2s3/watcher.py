import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

dumping_files = []


minioClient = Minio(
    'your_hostname.sampledomain.com:9000',
    access_key='ACCESS_KEY',
    secret_key='SECRET_KEY',
    secure=True,
    http_client=httpClient,
)


class FileCreateEventHandler(FileSystemEventHandler):
    events = 0

    def on_created(self, event):
        logger.info(f"{event.src_path} is created!")


def file_closed_interval_check(interval: int):
    """
    Check if current ``dumping_files`` is closed for every ``interval`` seconds.
    :returns : if closed, then it is ready to upload, yields the file path.
    """
    pass


def upload():
    pass


def main():
    pass


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "."

    event_handler = FileCreateEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    observer.join()
