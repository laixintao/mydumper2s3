import sys
import time
import logging
from threading import Thread

import psutil
import click
from minio import Minio
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
)


class FileCreateEventHandler(FileSystemEventHandler):
    events = 0

    def on_created(self, event):
        logger.info(f"new file created: {event.src_path} wait to close...")


def file_closed_interval_check_thread(interval: int):
    """
    Check if current ``dumping_files`` is closed for every ``interval`` seconds.
    :returns : if closed, then it is ready to upload, yields the file path.
    """
    def check_if_file_is_closed():
        while 1:
            for proc in psutil.process_iter():
                try:
                    for item in proc.open_files():
                        print(f"proc={proc} opened {item}")
                            
                except psutil.AccessDenied as e:
                    logger.warn(e)
                except Exception as e:
                    logger.warn(e)
            time.sleep(interval)
    return Thread(target=check_if_file_is_closed, name="file_closed_interval_check_thread")



def upload():
    pass


@click.command()
@click.option("--access_key")
@click.option("--secret_key")
@click.option("--domain")
@click.option("--bucket", help="is not spcified, a new bucket named by directory will be created")
@click.option("--path", default=".")
@click.option("--check-interval", default=1) # seconds
def main(access_key, secret_key, domain, bucket, path, check_interval):
    logger.info(f"upload {path} to {domain}/{bucket}, start to watch...")

    event_handler = FileCreateEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    file_closed_watcher = file_closed_interval_check_thread(check_interval)
    file_closed_watcher.start()

    observer.join()
    file_closed_watcher.join()


if __name__ == "__main__":
    main()
