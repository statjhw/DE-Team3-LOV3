# Airflow Service

포트홀 탐지 파이프라인 전체를 오케스트레이션하는 Airflow DAG입니다. Spark 클러스터 기동부터 데이터 처리, RDB 적재, 대시보드 갱신까지 14개 태스크를 순차 실행합니다.

## 역할

매일 하루치 센서 데이터를 배치 처리하는 파이프라인을 관리합니다. EC2 인스턴스의 시작/종료까지 DAG이 제어하여, 파이프라인이 돌지 않는 시간에는 클러스터를 꺼두어 비용을 절감합니다.

## 입출력

| | 항목 | 설명 |
|---|---|---|
| **Input** | S3 `raw-sensor-data/dt={날짜}/` | 차량 센서 데이터 (Parquet) |
| **Output** | PostgreSQL `pothole_segments` | 세그먼트별 일간 포트홀 집계 |
| **Output** | 3개 Materialized View 갱신 | 대시보드용 히트맵, 주간통계, 보수우선순위 |
| **Output** | 일일 이메일 리포트 | 당일 탐지 결과 요약 |

## DAG 구조

![DAG Graph](../docs/images/dag_graph.png)

## 파이프라인 흐름

14개 태스크를 4개 TaskGroup으로 구성하여 DAG Graph에서 논리적 단계를 한눈에 파악할 수 있습니다.

```
check_s3_input
  → [infra_setup]          EC2 시작 → Spark 기동 → 코드 배포 → 의존성 설치
      start_cluster → start_spark → download_code → install_deps
  → [spark_processing]     Stage1 이상탐지 → Stage2 공간클러스터링
      run_stage1 → check_s3_stage1_out → run_stage2 → check_s3_stage2_out
  → [serving]              PostgreSQL 적재 → MV 갱신 → 이메일 리포트
      load_to_rdb → refresh_views → send_email_report
  → [infra_cleanup]        Spark 종료 → EC2 중지 (always run)
      stop_spark → stop_cluster
```

## 핵심 설계

### EC2 온디맨드 기동/종료로 비용 최적화

Spark 클러스터(Master + Worker 4대)를 상시 운영하면 월 수십만 원의 EC2 비용이 발생합니다. DAG이 직접 `aws ec2 start-instances` / `stop-instances`를 호출하여 파이프라인 실행 시간에만 클러스터를 띄웁니다.

- `stop_spark`, `stop_cluster`는 `trigger_rule=all_done`으로 설정하여 **파이프라인 실패 시에도 반드시 종료**
- 인스턴스 시작 후 30초 네트워크 안정화 대기 포함

### Airflow 2.8.1 트리거 버그 우회

Airflow 2.8.1에서 `airflow dags trigger` CLI로 수동 트리거하면, `dag_hash` 최적화 로직의 버그로 TaskInstance가 생성되지 않아 DAG이 즉시 success 처리되는 문제가 있습니다.

`trigger_dag.py`에서 `dag_hash=None`으로 DagRun을 직접 DB에 생성하여 이 문제를 우회합니다.

### 각 스테이지 사이 S3 검증

Stage1 → Stage2 사이, Stage2 → RDB 적재 사이에 `build_s3_check_cmd()`로 출력 데이터 존재를 확인합니다. 중간 스테이지가 빈 결과를 내면 즉시 실패시켜 잘못된 데이터가 다운스트림으로 흘러가는 것을 방지합니다.

### SSH 기반 원격 실행

Airflow EC2에서 Spark Master와 Serving EC2에 SSH로 명령을 전달합니다. Airflow 자체에는 Spark나 PostgreSQL을 설치하지 않아 역할이 분리됩니다.

| Airflow Connection | 대상 | 용도 |
|---|---|---|
| `spark_master` | Spark Master EC2 | spark-submit, 클러스터 제어 |
| `serving_server` | Serving EC2 | RDB 적재, MV 갱신, 리포트 |

### spark-submit 명령 모듈화

`dag_utils.py`의 `build_spark_submit_cmd()`로 spark-submit 명령을 생성합니다. S3A 파일시스템 설정, Instance Profile 인증, AQE 옵션 등을 한 곳에서 관리하여, 새 Stage 추가 시 함수 호출 한 줄로 태스크를 생성할 수 있습니다.

## 구조

```
airflow_service/
├── dags/
│   ├── pothole_pipeline_dag.py   # DAG 정의 (4 TaskGroup, 14개 태스크)
│   └── dag_utils.py              # spark-submit, S3 검증 헬퍼
└── scripts/
    ├── run_pipeline.sh            # 파이프라인 실행 스크립트
    └── trigger_dag.py             # Airflow 2.8.1 버그 우회 트리거
```

## 실행 방법

```bash
bash scripts/run_pipeline.sh                       # 어제 날짜로 실행
bash scripts/run_pipeline.sh 2026-02-20            # 특정 날짜
bash scripts/run_pipeline.sh 2026-02-20 --force    # 기존 run 삭제 후 재실행
```

## 필요 환경변수

Airflow EC2의 `~/.bashrc`에 다음 환경변수가 등록되어 있어야 합니다. (`infra/env.sh` 참조)

| 변수 | 설명 |
|------|------|
| `MASTER_INSTANCE_ID` | Spark Master EC2 인스턴스 ID |
| `WORKER1~4_INSTANCE_ID` | Spark Worker EC2 인스턴스 ID |
| `MASTER_PRIVATE_IP` | Spark Master Private IP |
| `WORKER1~4_PRIVATE_IP` | Spark Worker Private IP |
| `AWS_REGION` | AWS 리전 (`ap-northeast-2`) |
| `S3_BUCKET` | 데이터 S3 버킷명 |
| `SLACK_WEBHOOK_URL` | Slack 장애 알림 (선택) |
