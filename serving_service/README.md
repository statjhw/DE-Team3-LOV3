# Serving Service

Spark 처리 결과와 외부 데이터를 PostgreSQL 데이터 마트에 적재하고, Materialized View로 대시보드에 제공하는 서빙 레이어입니다.

## 역할

파이프라인의 최종 단계로, 세 가지 책임을 가집니다.

1. **데이터 적재** — S3의 Stage2 결과를 PostgreSQL에 UPSERT, 공공 민원 데이터를 API로 수집하여 도로 세그먼트에 매핑
2. **데이터 마트 운영** — Fact/Dimension 테이블과 3개 Materialized View로 대시보드 쿼리 최적화
3. **시각화 및 리포트** — Streamlit 대시보드와 일일 이메일 리포트 제공

## 입출력

| | 항목 | 시점 | 설명 |
|---|---|---|---|
| **Input** | S3 `stage2_spatial_clustering/dt={날짜}/` | 매일 (Airflow DAG) | 세그먼트별 일간 포트홀 집계 (Parquet) |
| **Input** | 공공데이터 포트홀 민원 API | 수동/주기적 | 시민 민원 위치 데이터 |
| **Input** | Kakao Maps API | 신규 세그먼트 발생 시 | 세그먼트 좌표 → 도로명/행정구역 역지오코딩 |
| **Input** | `road_grade_863.csv` | 최초 1회 | 도로 위험도 등급 (road_network_builder 산출물) |
| **Output** | Streamlit 대시보드 | 상시 (MV 갱신 후 반영) | 히트맵, 보수 우선순위, 주간 통계 |
| **Output** | 일일 이메일 리포트 | 매일 (DAG 마지막 태스크) | Gmail SMTP 발송 |

## 데이터베이스 스키마

### 테이블

PostgreSQL `road_safety` 데이터베이스에 5개 테이블을 운영합니다.

| 테이블 | 유형 | PK | 설명 |
|--------|------|-----|------|
| `pothole_segments` | Fact (월별 파티션) | `(s_id, date)` | 세그먼트별 일간 충격 횟수/통행량 |
| `segment_address` | Dimension (SCD Type 1) | `s_id` | 도로명, 행정구역 (Kakao 역지오코딩) |
| `pothole_complaints` | Fact | `(create_dt, event_lat, event_lon)` | 공공 민원 데이터 |
| `segment_road_grade` | Dimension | `s_id` | 도로 위험도 등급 1~5 |
| `segment_repair_status` | Dimension | `s_id` | 보수 상태 (미보수/보수중/보수완료) |

### Materialized View

대시보드 응답 속도를 위해 3개 MV를 사전 집계하며, 데이터 적재 후 `REFRESH CONCURRENTLY`로 무중단 갱신합니다.

| MV | 용도 | 핵심 로직 |
|----|------|----------|
| `mvw_dashboard_heatmap` | 지도 히트맵 | 최신 날짜의 `risk_rate = impact_count / total_count × 100` |
| `mvw_dashboard_weekly_stats` | 주간 트렌드 | 최신 날짜 기준 7일간 요일별 통행량/충격 횟수 |
| `mvw_dashboard_repair_priority` | 보수 우선순위 | `priority_score = impacts×1 + complaints×50 + road_grade×10` |

## 핵심 설계

### 보수 우선순위 스코어링

단순히 포트홀 탐지 횟수만으로 보수 우선순위를 매기면 통행량이 많은 구간이 항상 상위에 올라갑니다. 이를 보완하기 위해 세 가지 요소를 가중 합산합니다.

```
priority_score = (sensor_impacts × 1.0) + (citizen_complaints × 50.0) + (road_grade × 10.0)
```

- **센서 탐지(×1)**: 차량 가속도계 기반 포트홀 탐지 횟수
- **시민 민원(×50)**: 공공데이터 포털의 실제 민원 건수. 민원 1건의 가중치를 센서 탐지 50건과 동일하게 설정하여, 시민이 체감하는 위험도를 반영
- **도로 등급(×10)**: road_network_builder가 산출한 1~5 등급. 좁은 도로, 저속 구간 등 구조적으로 위험한 도로에 가중치 부여

### 민원 데이터의 세그먼트 매핑

공공 민원 API의 좌표는 정확한 도로 위 좌표가 아닐 수 있습니다. cKDTree 공간 인덱스로 가장 가까운 세그먼트를 찾되, **500m 이내**인 것만 유효한 매핑으로 취급합니다. 위경도→미터 변환은 863호선 위도(~35.5°) 기준 근사식을 사용합니다.

### pothole_segments 월별 파티션

`pothole_segments`는 `PARTITION BY RANGE (date)`로 월 단위 파티셔닝되어 있습니다. 대시보드에서 날짜 범위 쿼리 시 해당 월의 파티션만 스캔하여 성능을 확보합니다. 매월 새 파티션을 수동 생성합니다.

### UPSERT 멱등성

`pothole_loader`는 `ON CONFLICT (s_id, date) DO UPDATE`로 적재합니다. 같은 날짜의 데이터를 재실행해도 중복 없이 최신 값으로 갱신되어, Airflow DAG의 `--force` 재실행이 안전합니다.

### 역지오코딩 최소 호출

Kakao Maps API는 호출량 제한이 있으므로, `segment_address`에 이미 등록된 세그먼트는 건너뛰고 **신규 세그먼트에 대해서만** 역지오코딩을 수행합니다.

## 대시보드

Streamlit 기반 대시보드로, MV에서 데이터를 조회합니다 (1시간 캐시).

- **히트맵**: Folium 지도 위에 세그먼트별 위험도를 시각화
- **보수 우선순위**: priority_score 기반 순위 테이블 (행 클릭 시 지도 이동)
- **주간 통계**: 요일별 통행량/충격 횟수 추이 차트

## 구조

```
serving_service/
├── loaders/
│   ├── base_loader.py          # DB 연결, 로깅 공통 클래스
│   ├── pothole_loader.py       # S3 Parquet → PostgreSQL UPSERT
│   └── complaint_loader.py     # 공공 민원 API → PostgreSQL
├── reporters/
│   └── email_reporter.py       # 일일 이메일 리포트 (Gmail SMTP)
├── dashboard/
│   └── app.py                  # Streamlit 대시보드
├── sql/
│   ├── 01_create_tables.sql    # 테이블 DDL (5개)
│   ├── 02_create_views.sql     # Materialized View DDL (3개)
│   └── 03_create_indexes.sql   # 인덱스 전략 (6개)
├── config.yaml                 # DB, S3, API 설정
├── docker-compose.yml          # PostgreSQL 15 컨테이너
└── requirements.txt
```

## 실행 방법

```bash
# PostgreSQL 기동
docker compose up -d

# 데이터 적재 (Airflow DAG이 자동 호출)
python -m loaders.pothole_loader --s3-path "s3://bucket/stage2/.../dt=2026-02-20/" --date 2026-02-20

# 민원 데이터 적재
python -m loaders.complaint_loader --service-key "API_KEY" --date-from 20260201 --date-to 20260220 --road-network road_network_863.csv

# 대시보드
streamlit run dashboard/app.py
```
