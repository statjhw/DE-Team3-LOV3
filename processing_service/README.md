# Processing Service

## 역할
Spark 기반 2단계 배치 처리 파이프라인입니다. 차량 센서 데이터에서 포트홀 충격 이벤트를 탐지(Stage 1)하고, 도로 세그먼트 단위로 공간 클러스터링(Stage 2)하여 일간 집계 결과를 생성합니다.

## 입출력

| | 항목 | 설명 |
|---|---|---|
| **Input** | S3 `raw-sensor-data/dt={날짜}/` | 차량 센서 데이터 (Parquet) |
| **Input** | S3 `road_network_863.csv` | 863호선 50m 세그먼트 좌표 |
| **Output** | S3 `stage2_spatial_clustered/dt={날짜}/` | 세그먼트별 일간 집계 결과 |

---

## Stage 1 — Anomaly Detection

### 1.1 데이터 수집 및 파티셔닝

- **Source Path** : `s3:/{bucket_name}/stage1_anomaly_detected/dt=YYYY-MM-DD/`
- **Format** : Parquet

**Input Schema**

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `timestamp` | TIMESTAMP | 센서 수집 시각 |
| `trip_id` | STRING | trip 식별자 |
| `vehicle_id` | STRING | 차량 식별자 |
| `accel_x`, `accel_y`, `accel_z` | DOUBLE | 3축 가속도 (m/s²) |
| `gyro_x`, `gyro_y`, `gyro_z` | DOUBLE | 3축 자이로스코프 (°/s) |
| `velocity` | DOUBLE | 주행 속도 (km/h) |
| `lon`, `lat` | DOUBLE | GPS 경·위도 |
| `hdop` | DOUBLE | GPS 수평 정밀도 저하율 (낮을수록 정확) |
| `satellites` | INT | GPS 수신 위성 수 |

**Output Schema**

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `timestamp` | TIMESTAMP | 센서 수집 시각 |
| `lon` | DOUBLE | GPS 경도 |
| `lat` | DOUBLE | GPS 위도 |
| `impact_score` | DOUBLE | 정규화 센서값 기반 충격 점수 |
| `is_pothole` | INT | 포트홀 여부 (1,0) |

### 1.2 로직

- **Step 1 : Context Filtering**
    - 조건 : `velocity <= 5.0` (정지 상태) **OR** `hdop > 5.0` **OR** `satellites < 4` (GPS 불량)
- **Step 2 : Z-Score 정규화**
    - 대상 : `accel_x/y/z`, `gyro_x/y/z`
    - `pyspark.ml.feature.StandardScaler` 사용 (Vehicle_id 기준 평균/분산 적용)
- **Step 3 : Impact Score 계산**

    ```python
    # 1. Config 로드 (Broadcast Variable)
    threshold = conf_broadcast.value['impact_threshold']
    w1, w2 = 0.7, 0.3

    # 2. Score 계산 (Vectorized Operation)
    from pyspark.sql import functions as F

    impact_score_expr = (F.abs(nor_az) * w1) + (F.abs(nor_gy) * w2)
    ```

---

## Stage 2 — Spatial Clustering

### 2.1 데이터 수집 및 조인 준비

**Input 1** : Stage 1 Output (S3 Intermediate Layer)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `timestamp` | TIMESTAMP | 센서 수집 시각 |
| `lon` | DOUBLE | GPS 경도 |
| `lat` | DOUBLE | GPS 위도 |
| `impact_score` | DOUBLE | 정규화 센서값 기반 충격 점수 |
| `is_pothole` | INT | 포트홀 여부 (1,0) |

**Input 2** : Road Network Data (S3 Storage) — 지방도 863호선 50m 단위 세그먼트 데이터

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `s_id` | STRING | 세그먼트(구역) ID |
| `start_lon` | DOUBLE | 시작점 GPS 경도 |
| `start_lat` | DOUBLE | 시작점 GPS 위도 |
| `end_lon` | DOUBLE | 끝점 GPS 경도 |
| `end_lat` | DOUBLE | 끝점 GPS 위도 |

**Output** : PostgreSQL `pothole_segments` — 세그먼트 단위(50m) 집계

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `s_id` | STRING | 세그먼트 ID |
| `centroid_lon` | DOUBLE | 세그먼트 중심 경도 |
| `centroid_lat` | DOUBLE | 세그먼트 중심 위도 |
| `date` | DATE | 집계 날짜 |
| `impact_count` | INT | impact_score 임계값 초과 포인트 수 (포트홀 탐지 횟수) |
| `total_count` | INT | 전체 포인트 수 (전체 포트홀 탐지 횟수) |

### 2.2 로직

- **Step 1: Spatial Join**
    - Stage 1에서 넘어온 각 포인트의 (`lon`, `lat`) 좌표를 도로 세그먼트에 매핑합니다.
    - 각 Row에 해당 포인트가 속한 `s_id` 컬럼을 추가합니다.
- **Step 2: 세그먼트별 군집화**
    - `s_id`를 기준으로 그룹화하여 해당 세그먼트에서 발생한 이상치 행과 전체 행의 개수를 `is_pothole` 으로 카운트합니다
- **Step 3: 최종 집계 및 저장**
    - 집계된 `impact_count, total_count` 및 메타데이터를 포함하여 S3에 저장합니다.
    - Source Path : `s3:/{bucket_name}/raw_sensor_data/dt=YYYY-MM-DD/`
