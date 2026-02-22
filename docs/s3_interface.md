# S3 인터페이스를 통한 모듈 간 의존성 분리

## 문제

파이프라인이 여러 서비스(수집 → 처리 → 서빙)로 나뉘어 있을 때, 각 서비스가 서로를 직접 호출하면 한 서비스의 장애가 전체 파이프라인으로 전파됩니다. 예를 들어 Processing 서비스가 Serving 서비스의 DB에 직접 INSERT하는 구조라면, DB가 다운되었을 때 Spark 잡까지 실패합니다.

## 설계: S3를 모듈 간 인터페이스로 사용

각 서비스는 **S3에 쓰기만 하고, 다음 서비스는 S3에서 읽기만** 합니다. 서비스 간에 직접 통신하지 않습니다.

```
S3 (raw-sensor-data)
    ↓
Processing Stage1 → S3 (stage1_anomaly_detected)
                        ↓
Processing Stage2 → S3 (stage2_spatial_clustering)
                        ↓
Serving Loader → PostgreSQL
```

### 각 서비스의 입출력

| 서비스 | 읽는 곳 | 쓰는 곳 |
|--------|---------|---------|
| Stage1 (Spark) | S3 `raw-sensor-data/` | S3 `stage1_anomaly_detected/` |
| Stage2 (Spark) | S3 `stage1_anomaly_detected/` | S3 `stage2_spatial_clustering/` |
| Serving Loader | S3 `stage2_spatial_clustering/` | PostgreSQL |

### 이 구조의 장점

**장애 격리**: Stage1이 실패해도 이전 단계의 raw 데이터는 S3에 그대로 남아있어 재처리가 가능합니다. Serving DB가 다운되어도 Stage2까지의 결과는 S3에 보존됩니다.

**독립 배포**: 각 서비스의 코드를 독립적으로 수정하고 배포할 수 있습니다. Stage1의 이상 탐지 로직을 변경해도 Stage2 코드를 수정할 필요가 없습니다. 서비스 간 계약은 S3의 Parquet 스키마뿐입니다.

**재처리 용이**: 특정 날짜의 데이터를 다시 처리하고 싶으면, 해당 날짜의 S3 경로만 지정하여 원하는 단계부터 재실행할 수 있습니다. Airflow DAG의 `--force` 옵션이 이를 지원합니다.

**디버깅**: 문제 발생 시 각 S3 경로의 Parquet 파일을 직접 확인하여 어느 단계에서 데이터가 잘못되었는지 빠르게 파악할 수 있습니다.

### S3 경로 규칙

모든 서비스가 `dt=YYYY-MM-DD/` 형식으로 날짜별 폴더를 나누어 저장합니다. Airflow의 `{{ ds }}` 템플릿 변수가 이 날짜를 일관되게 전달하여, 각 서비스가 어느 날짜의 데이터를 처리해야 하는지 명확합니다.

### S3 검증으로 단계 간 안전장치

서비스 간에 직접 통신하지 않는 대신, Airflow DAG이 각 단계 사이에 S3 출력 존재를 검증합니다.

```
run_stage1 → check_s3_stage1_out → run_stage2 → check_s3_stage2_out → load_to_rdb
```

이전 단계의 출력이 S3에 없으면 다음 단계를 시작하지 않아, 빈 데이터가 다운스트림으로 흘러가는 것을 방지합니다.
