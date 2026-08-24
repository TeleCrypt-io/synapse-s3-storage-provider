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
  store_remote: True
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

Regular cleanup job
-------------------

There is additionally a script at `scripts/s3_media_upload` which can be used
in a regular job to upload content to s3, then delete that from local disk.
This script can be used in combination with configuration for the storage
provider to pull media from s3, but upload it asynchronously.

Once the package is installed, the script should be run somewhat like the
following. We suggest using `tmux` or `screen` as these can take a long time
on larger servers.

`database.yaml` should contain the keys that would be passed to psycopg2 to
connect to your database. They can be found in the contents of the
`database`.`args` parameter in your homeserver.yaml.

More options are available in the command help.

```
> cd s3_media_upload
# cache.db will be created if absent. database.yaml is required to
# contain PG credentials
> ls
cache.db database.yaml
# Update cache from /path/to/media/store looking for files not used
# within 2 months
> s3_media_upload update /path/to/media/store 2m
Syncing files that haven't been accessed since: 2018-10-18 11:06:21.520602
Synced 0 new rows
100%|█████████████████████████████████████████████████████████████| 1074/1074 [00:33<00:00, 25.97files/s]
Updated 0 as deleted

> s3_media_upload upload /path/to/media/store matrix_s3_bucket_name --storage-class STANDARD_IA --delete
# prepare to wait a long time
```

Packaging and release
---------

For maintainers:

1. Update the `__version__` in setup.py. Then commit and push the changes:

    ```
    git add setup.py
    git commit -m "vX.Y.Z"
    git push
    ```

1. Create a signed tag and push that:

    ```
    git tag -s vX.Y.Z
    git push origin vX.Y.Z
    ```

1. [Create a release on GitHub](https://github.com/matrix-org/synapse-s3-storage-provider/releases/new) for this version.
1. When published, a [GitHub action workflow](https://github.com/matrix-org/synapse-s3-storage-provider/actions/workflows/release.yml) will build the package and upload to [PyPI](https://pypi.org/project/synapse-s3-storage-provider/).
