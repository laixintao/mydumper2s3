"""
watchdog -----------> mydumper_watcher ----------> uploading -------> done
dumping_files       delete from dumping_files     if all done
"""
import os
import sys
import time
import logging
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

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
uploading_files = []


class FileCreateEventHandler(FileSystemEventHandler):
    events = 0

    def on_created(self, event):
        logger.info(f"new file created: {event.src_path} wait to close...")
        dumping_files.append(event.src_path)


def _find_mydumper_pid():
    for proc in psutil.process_iter():
        try:
            if proc.name() != "mydumper":
                continue
            logger.info(f"Mydumper pid={proc.pid}")
            return proc
        except Exception as e:
            logger.warn(f"error when finding mydumper proc... {e}")


def watch_mydumper(interval: int, mydumper_proc, uploader):
    """
    Check if current ``dumping_files`` is closed for every ``interval`` seconds.
    :returns : if closed, then it is ready to upload, yields the file path.
    """
    while psutil.pid_exists(mydumper_proc.pid):
        logger.info(f"dump file check... (mydumper opened file: {len(dumping_files)}, uploading: {len(uploading_files)}).")
        mydumper_opened_files = []
        try:
            for item in mydumper_proc.open_files():
                mydumper_opened_files.append(item.path)
        except psutil.AccessDenied as e:
            pass
        except Exception as e:
            logger.warn(e)

        closed_files = [f for f in dumping_files if f not in mydumper_opened_files]
        if closed_files:
            logger.info(f"mydumper still open {', '.join(mydumper_opened_files)}")
            logger.info(
                f"mydumper no longer open {', '.join(closed_files)}, start to upload them to S3..."
            )
            for f in closed_files:
                uploader.upload(f)
                dumping_files.remove(f)

        time.sleep(interval)

    logger.info(f"Mydumper(pid={mydumper_proc.pid}) exit.")



class S3Uploader:
    def __init__(self, access_key, secret_key, domain, bucket, ssl, upload_thread):
        self.minio_client = Minio(
            domain, access_key=access_key, secret_key=secret_key, secure=ssl,
        )
        self.bucket = bucket
        self._ensure_bucket(bucket)
        self.executor = ThreadPoolExecutor(max_workers=upload_thread)

        # TODO delete file after upload(add flag!)

    def _ensure_bucket(self, bucket):
        found = self.minio_client.bucket_exists(bucket)
        if found:
            logger.info(f"bucket {bucket} already exist.")
        else:
            self.minio_client.make_bucket(bucket)
            logger.info(f"bucket {bucket} not exist... created one.")

    def upload(self, file_path):
        def _upload():
            try:
                logger.info(f"start upload {file_path}...")
                uploading_files.append(file_path)
                self.minio_client.fput_object(
                    self.bucket, os.path.basename(file_path), file_path
                )
                uploading_files.remove(file_path)
                logger.info(f"upload {file_path} done!")
            except Exception as e:
                logger.exception(e)

        self.executor.submit(_upload)

    def shutdown(self):
        """
        wait all uploading jobs to finish, then exit.
        """
        self.executor.shutdown(wait=True)


@click.command()
@click.option("--access_key")
@click.option("--secret_key")
@click.option("--domain")
@click.option(
    "--bucket", help="is not spcified, a new bucket named by directory will be created"
)
@click.option("--path", default=".")
@click.option("--check-interval", default=1)  # seconds
@click.option("--ssl/--no-ssl", default=False)
@click.option("--upload-thread", default=4)
def main(
    access_key, secret_key, domain, bucket, path, check_interval, ssl, upload_thread
):
    logger.info(f"upload {path} to {domain}/{bucket}, start to watch...")

    event_handler = FileCreateEventHandler()
    # add current exist files to watching list.
    dumping_files.extend([f"{os.path.abspath(path)}/{p}" for p in os.listdir(path)])
    logger.info(f"waiting mydumper to close {', '.join(dumping_files)}...")

    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    mydumper_proc = _find_mydumper_pid()
    if mydumper_proc is None:
        logger.error("Mydumper is not running!")
        return

    uploader = S3Uploader(access_key, secret_key, domain, bucket, ssl, upload_thread)

    try:
        watch_mydumper(check_interval, mydumper_proc, uploader)
    finally:
        time.sleep(1)  # give observer 1 more seconds to handle events.
        observer.stop()
    # upload left files on dumping_files
    for f in dumping_files:
        uploader.upload(f)
    uploader.shutdown()


if __name__ == "__main__":
    main()
