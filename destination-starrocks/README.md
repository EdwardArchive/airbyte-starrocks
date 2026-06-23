# destination-starrocks (Python) — cshift fork

Maintained fork of the Airbyte **StarRocks destination** connector, tracking the
deployed Python connector (upstream image `starrocks/destination-starrocks:1.0.0`,
which is NOT published as source on `StarRocks/airbyte-starrocks` — that repo's
`main` is the old Java connector). The Python sources here were imported from the
1.0.0 image so the connector can be patched and rebuilt.

## Why this fork exists
On our on-prem StarRocks (shared-data / CN) cluster the stock connector fails to
load data. Fixes are tracked as issues/PRs:

- **#-redirect — Stream Load auth lost on FE→CN 307 redirect** (✅ done): the
  Python `requests` session strips the `Authorization` header on cross-host
  redirects, so the CN node answers `no valid Basic authorization`. Fixed in
  `destination_starrocks/writer.py` (`_create_http_session`). Same root cause as
  upstream issue #6.
- **PRIMARY KEY tables** (planned): connector creates `UNIQUE KEY` tables;
  StarRocks recommends `PRIMARY KEY` (enables load-time delete + partial update).
- **`__op` delete** (planned): translate CDC `_ab_cdc_deleted_at` into a StarRocks
  Stream Load `__op` delete so deletes physically propagate.
- **CDC GLOBAL-state** (planned): make CDC syncs work end-to-end.

## Layout
```
destination-starrocks/
  main.py                       # entrypoint (python main.py)
  pyproject.toml / poetry.lock  # deps: airbyte-cdk 0.62.1, starrocks, requests
  Dockerfile                    # from-source build (canonical)
  Dockerfile.overlay            # quick build over the published 1.0.0 image
  destination_starrocks/        # connector module (config, destination, writer, run, spec)
```

## Build & push (internal Harbor; project `library` is public)
```bash
cd destination-starrocks
# canonical:
docker build -t 192.168.10.10/library/destination-starrocks:<tag> .
# or fast overlay:
docker build -f Dockerfile.overlay -t 192.168.10.10/library/destination-starrocks:<tag> .
docker login 192.168.10.10
docker push 192.168.10.10/library/destination-starrocks:<tag>
```
Then point the Airbyte custom connector at `192.168.10.10/library/destination-starrocks:<tag>`
(the image tag must match exactly, or the job pod hits ImagePullBackOff).

Currently deployed: `:1.0.1` (== `:1.0.1-redirectfix`).
