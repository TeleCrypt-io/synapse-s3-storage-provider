Synapse S3 Storage Provider
===========================

This module can be used by synapse as a storage provider, allowing it to fetch
and store media in Amazon S3.


Usage
-----

The `s3_storage_provider.py` should be on the PYTHONPATH when starting
synapse.

Example of entry in synapse config:

```yaml
media_storage_providers:
- module: s3_storage_provider.S3StorageProviderBackend
  store_local: True
  store_remote: False
  store_synchronous: True
  config:
    bucket: <S3_BUCKET_NAME>
    region_name: <S3_REGION_NAME>
    endpoint_url: https://s3.telecrypt.io
    access_key_id: <S3_ACCESS_KEY_ID>
    secret_access_key: <S3_SECRET_ACCESS_KEY>
```

The TeleCrypt fork accepts exactly these five values in `config`, and the
endpoint is fixed to `https://s3.telecrypt.io`. Legacy prefixes, session
tokens, encryption settings, storage classes, checksum settings, and unknown
keys are rejected at startup.

This module uses `boto3`, and so the credentials should be specified as
described [here](https://boto3.readthedocs.io/en/latest/guide/configuration.html#guide-configuration).

TeleCrypt's pinned Synapse fork passes the disposable upload source through
`FileInfo.upload_path`. The TeleCrypt release of this provider reads that path
directly and performs one ordinary S3 `PutObject`; it does not use boto3's
managed `upload_file` transfer or initiate multipart uploads. The optional
storage-provider deletion hook deletes the exact canonical key and treats an
already absent object as success.

The upload source must resolve beneath the runtime's disk-backed
`/staging/tmp` directory. The persistent compatibility path `/staging/media`,
the process temporary directory, and symlinks that resolve outside staging are
rejected before the object is written.

Legacy asynchronous migration/cleanup tooling is intentionally absent from
the TeleCrypt v1 package. It depended on a local media store, a separate
database credential file, and managed multipart uploads, which conflict with
v1's single synchronous `PutObject` path and disposable staging boundary.
Do not add a cleanup job or run an equivalent command against a v1 bucket.

Packaging and release
---------------------

The TeleCrypt v1 image consumes this provider as an exact source archive. Its
fork release is an annotated, immutable, non-prerelease GitHub Release with no
uploaded assets; the image workflow verifies that release and its peeled commit
before downloading the archive. The provider is not published to PyPI by this
fork workflow.
