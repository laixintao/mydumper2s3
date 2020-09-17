"""
watchdog -----------> mydumper_watcher ----------> uploading -------> done
dumping_files         delete from dumping_files       if all done
"""
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
    "your_hostname.sampledomain.com:9000",
    access_key="ACCESS_KEY",
    secret_key="SECRET_KEY",
    secure=True,
)


class FileCreateEventHandler(FileSystemEventHandler):
    events = 0

    def on_created(self, event):
        logger.info(f"new file created: {event.src_path} wait to close...")


def _find_mydumper_pid():
    for proc in psutil.process_iter():
        try:
            if proc.name() != "mydumper":
                continue
            logger.info(f"Mydumper pid={proc.pid}")
            return proc
        except Exception as e:
            logger.warn(f"error when finding mydumper proc... {e}")


def mydumper_watcher(interval: int, mydumper_proc, observer):
    """
    Check if current ``dumping_files`` is closed for every ``interval`` seconds.
    :returns : if closed, then it is ready to upload, yields the file path.
    """

    # mydumper
    # proc=psutil.Process(pid=84959, name='mydumper', status='running', started='13:40:38') opened popenfile(path='/Users/laixintao/Downloads/target/metadata.partial', fd=5)

    # find mydumper pid

    def check_if_file_is_closed():
        while psutil.pid_exists(mydumper_proc.pid):
            logger.info("----")
            for item in mydumper_proc.open_files():
                try:
                    print(f"mydumper opened {item}...")

                except psutil.AccessDenied as e:
                    pass
                except Exception as e:
                    logger.warn(e)
            time.sleep(interval)
        logger.info(f"Mydumper(pid={mydumper_proc.pid}) exit.")
        # TODO upload left files on dumping_files
        observer.stop()

    return Thread(target=check_if_file_is_closed, name="mydumper_watcher")


def upload():
    pass


@click.command()
@click.option("--access_key")
@click.option("--secret_key")
@click.option("--domain")
@click.option(
    "--bucket", help="is not spcified, a new bucket named by directory will be created"
)
@click.option("--path", default=".")
@click.option("--check-interval", default=1)  # seconds
def main(access_key, secret_key, domain, bucket, path, check_interval):
    logger.info(f"upload {path} to {domain}/{bucket}, start to watch...")

    event_handler = FileCreateEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    mydumper_proc = _find_mydumper_pid()
    if mydumper_proc is None:
        logger.error("Mydumper is not running!")
        return

    mydumper_watcher_thread = mydumper_watcher(check_interval, mydumper_proc, observer)
    mydumper_watcher_thread.start()

    mydumper_watcher_thread.join()
    observer.join()


if __name__ == "__main__":
    main()
