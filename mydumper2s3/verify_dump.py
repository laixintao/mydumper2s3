import os
import click
import hashlib
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
@click.option("--path", default=".")
@click.option("--ssl/--no-ssl", default=False)
def main(access_key, secret_key, domain, path, bucket, ssl):
    minio_client = Minio(
        domain, access_key=access_key, secret_key=secret_key, secure=ssl
    )
    objects = list(minio_client.list_objects(bucket))

    if file_list_check(objects, path):
        print("All files are exist both on local and on S3, file name check pass...")
    else:
        print("File name check failed!")
        return

    print(f"start verifying file's md5, file count: {len(objects)}.")
    success_count = fail_count = 0
    for obj in objects:
        print(
            f"({success_count+fail_count+1}/{len(objects)}) verifying {obj.object_name}...",
            end="",
            flush=True,
        )
        if "-" in obj.etag:
            print(
                " file uploaded by multipart, downloading file...", end="", flush=True
            )
            content = minio_client.get_object(bucket, obj.object_name).read()
            object_hash = hashlib.md5(content).hexdigest()
            print(" done...", end="", flush=True)
        else:
            object_hash = obj.etag

        with open(f"{path}/{obj.object_name}", "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        if file_hash == object_hash:
            print("pass")
            success_count += 1
        else:
            print("fail!")
            fail_count += 1
    print(f"verify finished, passed: {success_count}, failed: {fail_count}.")


if __name__ == "__main__":
    main()
