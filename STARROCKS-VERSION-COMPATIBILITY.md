# StarRocks 버전 호환성 — destination-starrocks (Stream Load / Primary Key / CDC)

> **상태:** 초안(untracked). 이 문서는 **Python/Stream Load 커넥터**의 동작을 다룹니다.
> 이 repo `main`은 구 Java 커넥터이므로, 최종 위치는 Python 커넥터 소스 옆
> (`python-connector` 브랜치) 또는 repo 레벨 `docs/`로 옮기는 것을 권장합니다.
>
> **전제:** 대상 클러스터는 **shared-data 모드**(FE + CN, object storage). 따라서
> 기능 게이팅 기준은 일반 PK 문서가 아니라 **shared-data 전용 지원표**입니다.
> 일부 PK 기능은 shared-data에서 더 늦게 들어왔습니다.

## 0. 요약

- 커넥터의 근간(**PRIMARY KEY 테이블 + `__op` upsert/delete + `merge_condition`**)은
  **shared-data 3.3.11 이상에서 전부 사용 가능**합니다. 이 값을 **baseline**으로 잡습니다.
- 버전을 올릴수록 **새 기능이 아니라 처리량/운영 편의**가 좋아집니다
  (3.4 Merge Commit → 3.5 자동 모드 판별 → 4.1 PK 적재 병렬화).
- 커넥터는 플래그 더미 대신 **버전을 감지(`SELECT current_version()`)**하여,
  지원되지 않는 조합을 **fail-fast/경고**하고 이 문서를 근거로 동작을 맞춥니다.

## 1. 버전 감지

커넥터는 DDL을 위해 이미 **9030(MySQL 프로토콜)**에 접속합니다. 그 연결에서:

```sql
SELECT current_version();   -- 예: "3.3.11" 또는 "3.5.4" 또는 "4.1.1"
```

`check`/startup 단계에서 버전을 읽어 아래 매트릭스와 대조합니다.

- 지원되지 않는 조합을 켰으면 **명확한 에러로 중단**
  (예: `merge_condition requires shared-data >= 3.1; column-mode partial + condition requires >= 3.3.11; detected 3.3.5`).
- shared-data 표 기준 미달이면 해당 동작을 **비활성 + 경고 로그**.

> 이유: 버전 미스매치는 런타임에 정체불명의 Stream Load 4xx로 나타나 디버깅이 어렵습니다.
> 적재 전에 9030으로 한 번 검증하는 것이 비용 대비 효과가 큽니다.

## 2. 버전 호환성 매트릭스 (shared-data 기준)

범례: ✅ 사용 가능 · ⚠️ 조건부/주의 · ❌ 미지원(상위 버전 필요)

| 기능 (커넥터 용도) | shared-data 도입 | **3.3.11** | **3.5.x** | **4.1.1** |
|---|---|:---:|:---:|:---:|
| PRIMARY KEY 테이블 (#3) | v3.1.0 | ✅ | ✅ | ✅ |
| `__op` upsert/delete (#4) | v3.1.0 (PK와 함께) | ✅ | ✅ | ✅ |
| `merge_condition` 풀-업서트 (#5 순서안전) | v3.1.0 (**Greater(≥)만**) | ✅ | ✅ | ✅ |
| partial update — row mode | v3.1.0 | ✅ | ✅ | ✅ |
| partial update — column mode | v3.3.1 | ✅ | ✅ | ✅ |
| column-mode partial **+ conditional 조합** | v3.3.11 | ✅ | ✅ | ✅ (가장 견고) |
| 전송 압축 (gzip/lz4_frame/zstd 등) | v3.3.2 | ✅ | ✅ | ✅ |
| persistent index = `CLOUD_NATIVE`(object store) | v3.3.2 / 기본값 v3.3.8 | ⚠️ ≥3.3.8 권장 | ✅ | ✅ |
| **Merge Commit** (동시 소형 적재 병합) | v3.4.0 | ❌ | ✅ | ✅ |
| partial-update **모드 자동 판별** | v3.5.8 / 4.0.2 | ❌ (수동 지정) | ✅ ≥3.5.8 | ✅ |
| **PK 적재 병렬화** (publish/index/conditional) | v4.1.0 | ❌ | ❌ | ✅ |

### 버전별 한 줄 요약

- **3.3.11 (baseline):** 커넥터 전체 기능셋이 *가능*. 단 Merge Commit·자동모드·병렬적재가
  없어 CDC 적재 효율/운영은 가장 불리. 이미 3.3이 깔려 있을 때의 하한선.
- **3.5.x (현 stable, 권장):** 3.3 전체 + **Merge Commit**(소형 배치 버전압박 완화) +
  **자동 모드 판별**. 안정성·효율의 sweet spot. ⚠️ **3.5.0부터 JDK 17+ 필수**.
- **4.1.1 (latest):** + **PK 적재 병렬화**로 처리량 천장이 가장 높음, column-mode+conditional이
  가장 견고. Bleeding edge.

## 3. 플래그가 아니라 "로직"이 필요한 주의점 (모든 버전 공통)

버전과 무관하게 항상 지키는 제약. spec 필드 추가로 끝나지 않습니다.

1. **`merge_condition`은 'Greater(≥)' 연산만** 지원
   → #5의 기준 컬럼(`dedup_ordering_column`)은 **단조 증가**여야 함.
   CDC binlog 위치(`_ab_cdc_log_file` + `_ab_cdc_log_pos`)에서 파생한 단조 시퀀스가 이상적.
   단순 `updated_at` 타임스탬프는 동률/시계오차로 깨질 수 있어 **검증/경고** 대상.

2. **`merge_condition`은 DELETE(`__op=1`)에 적용되지 않음** + 한 배치 내 조건 컬럼 혼용 제약
   → 조건부 **upsert 배치와 delete 배치를 분리 적재**해야 함.
   안 그러면 순서 역전된 delete가 나중에 재삽입된 행을 삭제할 수 있음.

3. **`__op`은 v3.3.6+에서 컬럼명으로 예약** → 소스에 `__op` 컬럼이 있으면 충돌(개명 필요).

## 4. 권장 타깃

- **안정 우선:** **3.5.x** — 기능 전체 + Merge Commit + 자동 모드, production-stable.
- **처리량 우선:** **4.1.1** — PK 적재 병렬화 + 가장 견고한 column-mode+conditional.
- **하한선:** **3.3.11** — 그 미만은 column-mode+conditional 조합이 불완전하므로 지원하지 않음.

## 5. 운영 노트

- **JDK 17+**: 3.5.0 이상 클러스터 업그레이드 시 필요(클러스터 측 요건).
- **persistent index 기본값**: 3.3.8+ shared-data PK는 `CLOUD_NATIVE`가 기본.
- **Merge Commit**(3.4+): 헤더 `enable_merge_commit` 등으로 활성. Airbyte의 잦은 소형 배치에서
  버전 폭발/compaction 압박을 줄임. 3.3.x에선 불가하므로 배치 크기/빈도로 대체.

## 6. 출처

- Stream Load SQL 레퍼런스 — https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/STREAM_LOAD/
- Load to Primary Key tables — https://docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/
- Feature Support: Shared-data Clusters — https://docs.starrocks.io/docs/deployment/shared_data/feature-support-shared-data/
- Feature Support: Loading/Unloading — https://docs.starrocks.io/docs/loading/loading_introduction/feature-support-loading-and-unloading/
- Release notes — 3.3 / 3.4 / 3.5 / 4.0 / 4.1 (docs.starrocks.io/releasenotes/)
- 관련 PR: column-mode partial in shared-data (#46516), shared-data conditional partial (#56132),
  CLOUD_NATIVE default (#52209), `__op` reserved name (#52621), 4.1 Lake column-mode conditional (#71961)
