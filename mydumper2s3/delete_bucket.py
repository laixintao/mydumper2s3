"""
delete all files from a bucket.
then delete the bucket.
"""
import os
import click
from minio import Minio


def file_list_check(objects, path):
    object_names = [o.object_name for o in objects]
    files = os.listdir(path)
    only_in_objects = [o.object_name for o in objects if o.object_name not in files]
    only_in_local = [f for f in files if f not in object_names]

    if only_in_objects:
        print(f"files only exist on S3: {only_in_objects}")
        return False
    if only_in_local:
        print(f"files only exist on local: {only_in_local}")
        return False
    return True


@click.command()
@click.option("--access_key")
@click.option("--secret_key")
@click.option("--domain")
@click.option(
    "--bucket", help="is not spcified, a new bucket named by directory will be created"
)
@click.option("--ssl/--no-ssl", default=False)
def main(access_key, secret_key, domain, bucket, ssl):
    minio_client = Minio(
        domain, access_key=access_key, secret_key=secret_key, secure=ssl
    )
    objects = list(minio_client.list_objects(bucket))
    print(f"{len(objects)} will be removed...")
    resp = minio_client.remove_objects(bucket, [o.object_name for o in objects])
    print(f"errors: {list(resp)}")
    print(f"bucket {bucket} is clean, now remove the bucket...")
    minio_client.remove_bucket(bucket)
    print("done!")


if __name__ == "__main__":
    main()
