# Mysql2s3

一个 MySQL 备份上传的工具。

## Run monio locally

```
docker run -p 9000:9000 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /data
```

## TODO

- [ ] ci and test
- [ ] 断点续传?
- [ ] 支持 gzip 选项？(Mydumper 本身支持)
- [ ] 数据的hash check
- [ ] 是否要分片上传，支持并行下载，这样恢复数据的时候更快
- [ ] 使用一个 meta file 来存储index以及hash等；
