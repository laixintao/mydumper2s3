"""
dumping_files -> uploading_files -> uploaded_files

if delete:
list_files = dumping_files + uploading_files

if not delete:
(final) list_files = uploaded_files
"""
import os
import sys
import time
import logging
import urllib3
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import psutil
import click
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="/tmp/mydumper2s3.log",
)
logger = logging.getLogger(__name__)

DELETE_AFTER_UPLOAD = False

refresh_stat_lock = Lock()
# files in target directory
list_files = []
# files opened by mydumper
dumping_files = []
# files are loading to s3
uploading_files = set()
uploading_files_lock = Lock()
# files finished uploading
uploaded_files = []


def _find_mydumper_pid():
    for proc in psutil.process_iter():
        try:
            if proc.name() != "mydumper":
                continue
            logger.info(f"Mydumper pid={proc.pid}")
            return proc
        except Exception as e:
            logger.warn(f"error when finding mydumper proc... {e}")


def watch_mydumper(interval: int, mydumper_proc, uploader, path):
    """
    Check if current ``dumping_files`` is closed for every ``interval`` seconds.
    :returns : if closed, then it is ready to upload, yields the file path.
    """
    while psutil.pid_exists(mydumper_proc.pid):
        logger.info(
            f"dump file check... (mydumper opened file: {len(dumping_files)}, uploading: {len(uploading_files)})."
        )
        for f in scan_uploadable_files(path, mydumper_proc):
            uploader.upload(f)

        time.sleep(interval)

    logger.info(f"Mydumper(pid={mydumper_proc.pid}) exit.")


class S3Uploader:
    def __init__(self, access_key, secret_key, domain, bucket, ssl, upload_thread):
        self.minio_client = Minio(
            domain,
            access_key=access_key,
            secret_key=secret_key,
            secure=ssl,
            http_client=urllib3.poolmanager.PoolManager(maxsize=upload_thread),
        )
        self.domain = domain
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = ssl
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
        with uploading_files_lock:
            uploading_files.add(file_path)

        def _upload():
            try:
                start_time = time.time()
                logger.info(f"start upload {file_path}...")
                refresh_stats()
                self.minio_client.fput_object(
                    self.bucket, os.path.basename(file_path), file_path
                )
                with uploading_files_lock:
                    uploading_files.remove(file_path)
                uploaded_files.append(file_path)
                refresh_stats()
                end_time = time.time()
                logger.info(
                    f"upload {file_path} done! cost: {end_time-start_time} seconds."
                )
            except Exception as e:
                logger.exception(e)

        self.executor.submit(_upload)

    def shutdown(self):
        """
        wait all uploading jobs to finish, then exit.
        """
        self.executor.shutdown(wait=True)


def scan_uploadable_files(path, mydumper_proc):
    """
    :returns: files that not uplaoded yet, and not opened by mydumper.
    """
    global list_files, dumping_files
    list_files = [f"{os.path.abspath(path)}/{p}" for p in os.listdir(path)]

    if not mydumper_proc:
        # upload all
        return [
            f
            for f in list_files
            if f not in uploaded_files and f not in uploading_files
        ]

    mydumper_opened_files = []
    try:
        for item in mydumper_proc.open_files():
            mydumper_opened_files.append(item.path)
    except psutil.AccessDenied:
        pass
    except Exception as e:
        logger.warn(e)

    dumping_files = mydumper_opened_files
    refresh_stats()
    ready_to_upload = [
        f
        for f in list_files
        if f not in uploaded_files
        and f not in mydumper_opened_files
        and f not in uploading_files
    ]
    logger.info(f"=ready to upload files: {ready_to_upload}")
    return ready_to_upload


def refresh_stats():
    global list_files, dumping_files, uploaded_files, uploaded_files
    global DELETE_AFTER_UPLOAD
    with refresh_stat_lock:
        text = (
            f"\r{len(list_files):>4} files in directory,"
            f"{len(dumping_files):>4} dumping,"
            f"{len(uploading_files):>4} uploading,"
            f"{len(uploaded_files):>4} uploaded"
        )
        if DELETE_AFTER_UPLOAD:
            text += "(deleted)."
        else:
            text += "."
        sys.stdout.write(text)


@click.command()
@click.option("-a", "--access_key", prompt=True, help="S3 access_key")
@click.option("-s", "--secret_key", prompt=True, help="S3 secret_key")
@click.option("-d", "--domain", help="S3 domain", prompt=True)
@click.option(
    "-b",
    "--bucket",
    help="S3 bucket, if not spcified, a new bucket named by directory will be created",
    prompt=True,
)
@click.option("-l", "--path", default=".")
@click.option("-i", "--check-interval", default=1)  # seconds
@click.option("--ssl/--no-ssl", default=False)
@click.option(
    "-t", "--upload-thread", default=4, help="thread numbers used to upload to s3"
)
def main(
    access_key, secret_key, domain, bucket, path, check_interval, ssl, upload_thread
):
    """
    mydumper2s3: upload mydumper dumped files to s3 bucket.
    It works even while mydumper is running!
    """
    global dumping_files, uploading_files, list_files
    logger.info(f"upload {path} to {domain}/{bucket}...")

    # add current exist files to watching list.
    list_files = [f"{os.path.abspath(path)}/{p}" for p in os.listdir(path)]

    # if mydumper_proc doesn't exist, download all files then exit.
    # otherwiase, watching for every ``interval`` seconds.
    mydumper_proc = _find_mydumper_pid()
    if mydumper_proc is None and not list_files:
        print("there is nothing to upload.")
        return

    uploader = S3Uploader(access_key, secret_key, domain, bucket, ssl, upload_thread)
    if mydumper_proc is None:
        print("mydumper is not running, I just upload exist files then exist...")
        for f in list_files:
            uploader.upload(f)
            refresh_stats()

    else:
        watch_mydumper(check_interval, mydumper_proc, uploader, path)

        # upload left files on dumping_files
        for f in scan_uploadable_files(path, mydumper_proc):
            uploader.upload(f)
    uploader.shutdown()
    print(f"\n{len(uploaded_files)} files successfully uploaded.")


if __name__ == "__main__":
    main()
