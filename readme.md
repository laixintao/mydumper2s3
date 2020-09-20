# Mydumper To S3

![Action](https://github.com/laixintao/mydumper2s3/workflows/Test/badge.svg)

A tool that can upload mydumper dumped files to S3 bucket.

![](./docs/mydumper2s3.gif)

It works even while mydumper is running, for mydumper opened files, mydumper2s3
will wait mydumper to close those files then upload. `--delete-after-upload`
option enables you to backup your MySQL without dumping all of your data to
local disk.

It works like this:

```

+-----------+
|  mydumper |
+-----+-----+
      |
      |
      v
+-----+------+  upload  +-------------+      +------+
| local disk +----------> mydumper2s3 +------>  s3  |
|            <----------+             |      |bucket|
+------------+  delete  +-------------+      +------+
                after
                upload
```

## Install

```
   pip install mydumper2s3
```

## Usage

Check help:

```
$ mydumper2s3 --help
Usage: mydumper2s3.py [OPTIONS]

  mydumper2s3: upload mydumper dumped files to s3 bucket. It works even
  while mydumper is running!

Options:
  -a, --access_key TEXT           S3 access_key
  -s, --secret_key TEXT           S3 secret_key
  -d, --domain TEXT               S3 domain
  -b, --bucket TEXT               S3 bucket, if not spcified, a new bucket
                                  named by directory will be created

  -l, --path TEXT
  -i, --check-interval INTEGER
  --ssl / --no-ssl
  -t, --upload-thread INTEGER     thread numbers used to upload to s3
  --delete-after-upload / --no-delete-after-upload
                                  if set to True, files will be deleted in
                                  local space after uploading.

  --help                          Show this message and exit.
```

### Example

Upload files to S3 (If mydumper is running, you can still using this command,
mydumper2s3 will search mydumper process pid and watch files opended by
mydumper.):

```
$ mydumper2s3 --domain 127.0.0.1:9000 \
             --bucket test1 \
             --path ~/Downloads/target \
             --access_key AKIAIOSFODNN7EXAMPLE \
             --secret_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
             --upload-thread=10
138 files in directory,   0 dumping,   0 uploading,  138 uploaded.
138 files successfully uploaded.
```

You can check the backup with this command (available in your $PATH after install mydumper2s3):

```
$ verify-dump --domain 127.0.0.1:9000 \
              --bucket test1 \
              --path ~/Downloads/target \
              --access_key AKIAIOSFODNN7EXAMPLE \
              --secret_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
All files are exist both on local and on S3, file name check pass...
start verifying file's md5, file count: 138.
(1/138) verifying metadata...pass
(2/138) verifying test-schema-create.sql...pass
(3/138) verifying test.foo_event-schema.sql...pass
(4/138) verifying test.foo_event_alarms-schema.sql...pass
(5/138) verifying test.foo_list-schema.sql...pass
…
```

# Development

## Run monio locally

```
docker run -p 9000:9000 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /data
```
