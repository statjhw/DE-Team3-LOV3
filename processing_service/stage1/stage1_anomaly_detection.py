from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType,
)
from pyspark.sql.utils import AnalysisException
from typing import Dict, Any, Iterator
from datetime import date,datetime, timedelta
import os
import logging
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

#output schema
def get_output_schema() -> StructType:
    return StructType([
        StructField("timestamp", LongType(), False),
        StructField("lon", DoubleType(), False),
        StructField("lat", DoubleType(), False),
        StructField("impact_score", DoubleType(), False),
        StructField("is_pothole", IntegerType(), False),
    ])

#input schema
def get_input_schema() -> StructType:
    return StructType([
        StructField("timestamp", LongType(), False),
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("accel_x", DoubleType(), False),
        StructField("accel_y", DoubleType(), False),
        StructField("accel_z", DoubleType(), False),
        StructField("gyro_x", DoubleType(), False),
        StructField("gyro_y", DoubleType(), False),
        StructField("gyro_z", DoubleType(), False),
        StructField("velocity", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("lat", DoubleType(), False),
        StructField("hdop", DoubleType(), False),
        StructField("satellites", IntegerType(), False),
    ])

#batch date
def _batch_date(batch_date_str: str = None) -> str:
    if batch_date_str and batch_date_str.strip():
        return batch_date_str.strip()
    env_date = os.getenv("BATCH_DATE", "").strip()
    if env_date:
        return env_date
    kst_now = datetime.now(ZoneInfo('Asia/Seoul'))
    yesterday = kst_now - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

class AnomalyDetectionPipeline:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        # spark config 모든 워커 노드에 broadcasting
        self.config_broadcast = spark.sparkContext.broadcast(config)
        self._validate_config()

    # input 데이터 정합성 검증
    def _validate_config(self) -> None:
        cfg = self.config_broadcast.value
        required_keys = {
            "context_filtering": ["velocity_threshold", "hdop_threshold", "min_satellites"],
            "impact_score": ["weights", "threshold"],
        }
        for section, keys in required_keys.items():
            if section not in cfg:
                raise ValueError(f"필수 설정 누락 : {section}")
            for key in keys:
                if key not in cfg[section]:
                    raise ValueError(f"필수 설정 키 누락: {section}.{key}")
        if "accel_z" not in cfg["impact_score"].get("weights", {}):
            raise ValueError("필수 가중치 누락: impact_score.weights.accel_z")
        if "gyro_y" not in cfg["impact_score"].get("weights", {}):
            raise ValueError("필수 가중치 누락: impact_score.weights.gyro_y")

    # 이상치 제거 -> z값 계산 진행
    def run(self,input_df: DataFrame) -> DataFrame:
        filtered_df = self._context_filtering(input_df)
        return self._compute_anomaly_scores(filtered_df)
    
    # GPS 이상치 필터링 
    def _context_filtering(self, df: DataFrame) -> DataFrame:
        cfg = self.config_broadcast.value.get("context_filtering", {})
        v_th = cfg.get("velocity_threshold")
        h_th = cfg.get("hdop_threshold")
        s_min = cfg.get("min_satellites")

        if v_th is None or h_th is None or s_min is None:
            raise ValueError(
                "context_filtering 설정 키 누락: velocity_threshold, hdop_threshold, min_satellites"
            )

        return df.filter(
            (F.col("velocity") > v_th) &
            (F.col("hdop") <= h_th) &
            (F.col("satellites") >= s_min)
        )

    # 방지턱 데이터 로드
    def _load_speed_bump_data(self) -> DataFrame:
        speed_bump_path = self.config_broadcast.value.get("storage", {}).get("speed_bump_path")
        bump_schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("WGS84위도", StringType(), True),
            StructField("WGS84경도", StringType(), True),
            StructField("과속방지턱폭", StringType(), True),
            StructField("과속방지턱연장", StringType(), True),
            StructField("도로유형구분", StringType(), True)
        ])
        return self.spark.read.schema(bump_schema).option("header", "true").csv(speed_bump_path) \
            .select(
                F.col("WGS84위도").cast("double").alias("bump_lat"),
                F.col("WGS84경도").cast("double").alias("bump_lon"),
                F.col("과속방지턱연장").cast("double").alias("bump_length")
            )
    
    def _compute_anomaly_scores(self, df: DataFrame) -> DataFrame:
        # 임계치, 가중치 로드
        cfg = self.config_broadcast.value.get("impact_score", {})
        weights = cfg.get("weights", {})
        if weights.get("accel_z") is None or weights.get("gyro_y") is None or cfg.get("threshold") is None:
            raise ValueError(
                "impact_score 설정 키 누락: weights.accel_z, weights.gyro_y, threshold"
            )
        w1 = float(weights["accel_z"])
        w2 = float(weights["gyro_y"])
        threshold = float(cfg["threshold"])
        # 윈도우 함수 사용하여 같은 trip_id 한 파티션에 로드
        trip_window = Window.partitionBy("trip_id")

        def safe_zscore(col_name: str, mean_expr, std_expr):
            return F.when(
                std_expr.isNull() | (std_expr == 0), 0.0
            ).otherwise(
                (F.col(col_name) - mean_expr) / std_expr
            )
        # impact_score 계산
        mean_az = F.mean("accel_z").over(trip_window)
        std_az  = F.stddev("accel_z").over(trip_window)
        mean_gy = F.mean("gyro_y").over(trip_window)
        std_gy  = F.stddev("gyro_y").over(trip_window)

        nor_az = safe_zscore("accel_z", mean_az, std_az)
        nor_gy = safe_zscore("gyro_y",  mean_gy, std_gy)

        impact_score_expr = (F.abs(nor_az) * w1) + (F.abs(nor_gy) * w2)

        scored_df = df.select(
            "timestamp", "lon", "lat",
            impact_score_expr.alias("impact_score")
        )
        # 방지턱 데이터 join 전 임계치 이상 레코드 분리
        normal_df = scored_df.filter(F.col("impact_score") <= threshold).withColumn("is_pothole", F.lit(0).cast("int"))

        suspect_df = scored_df.filter(F.col("impact_score") > threshold)

        speed_bump_df = self._load_speed_bump_data()
        LENGTH_FACTOR = 1.0 / (2.0 * 100.0 * 111000.0)

        # 방지턱 데이터 조인하여 방지턱으로 인한 충격 필터링
        joined = suspect_df.join(
            F.broadcast(speed_bump_df),
            (
                (F.abs(F.col("lat") - F.col("bump_lat")) <= F.col("bump_length") * LENGTH_FACTOR) &
                (F.abs(F.col("lon") - F.col("bump_lon")) * F.cos(F.radians(F.col("lat"))) <= F.col("bump_length") * LENGTH_FACTOR)
            ),
            "left",
        )

        anomaly_df = joined.groupBy("timestamp", "lon", "lat", "impact_score") \
            .agg(F.max(F.when(F.col("bump_lat").isNotNull(), 1).otherwise(0)).alias("is_near_bump")) \
            .select(
                "timestamp", "lon", "lat", "impact_score",
                F.when(F.col("is_near_bump") == 1, 0).otherwise(1).cast("int").alias("is_pothole")
            )
        # 최종 결과 union
        return normal_df.unionByName(anomaly_df)


def run_job(
    spark: SparkSession,
    config: Dict[str, Any],
    input_base_path: str,
    output_base_path: str,
    batch_date: Optional[str] = None,
) -> None:
    batch_dt = _batch_date(batch_date)
    input_base_path = input_base_path.rstrip("/")
    output_base_path = output_base_path.rstrip("/")
    partition_input = f"{input_base_path}/dt={batch_dt}"
    partition_output = f"{output_base_path}/dt={batch_dt}"

    logger.info("배치 날짜: %s", batch_dt)
    logger.info("입력: %s", partition_input)
    logger.info("출력: %s", partition_output)

    try:
        input_df = spark.read.schema(get_input_schema()).parquet(partition_input)
    except AnalysisException as e:
        logger.error("입력 경로 읽기 실패 (파일 없음?): %s — 배치 스킵", partition_input)
        raise RuntimeError(f"입력 데이터 없음: {partition_input}") from e

    try:
        pipeline = AnomalyDetectionPipeline(spark, config)
    except ValueError as e:
        logger.error("파이프라인 설정 오류: %s", str(e))
        raise

    result_df = pipeline.run(input_df)

    spark_config = config.get("spark", {})
    target_partitions = spark_config.get("coalesce_partitions", 4)
    result_df = result_df.coalesce(target_partitions)

    # cache->count()->write() 순서로 cache 활용
    result_df = result_df.cache()

    # 로그: 빈 결과 경고
    final_count = result_df.count()
    if final_count == 0:
        logger.warning("최종 결과가 비어 있음")
    else:
        logger.info("완료: 출력 %d 레코드 저장", final_count)

    # write
    result_df.write.mode("overwrite").option("compression", "snappy").parquet(partition_output)
    logger.info("출력 저장 완료: %s", partition_output)
    
    result_df.unpersist()


if __name__ == "__main__":
    import argparse
    import sys

    _stage1_dir = os.path.dirname(os.path.abspath(__file__))
    if _stage1_dir not in sys.path:
        sys.path.insert(0, _stage1_dir)

    from connection_stage1 import load_config, get_spark_session

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Stage 1: Anomaly Detection")
    parser.add_argument("--env", required=True, choices=["local", "stage1", "prod"])
    parser.add_argument("--batch-date", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    config_path = os.path.join(_stage1_dir, f"config_{args.env}.yaml")
    
    if not os.path.isfile(config_path):
        logger.warning("로컬 설정 파일 없음: %s, S3에서 로드 시도", config_path)
        config_path = f"s3a://softeer-7-de3-bucket/scripts/config_{args.env}.yaml"

    config = load_config(config_path)
    storage = config.get("storage", {})
    input_base = storage.get("input_base_path", "./data/raw-sensor-data")
    output_base = storage.get("output_base_path", "./data/stage1_anomaly_detected")

    spark = get_spark_session(config)
    try:
        run_job(spark, config, input_base, output_base, args.batch_date)
    finally:
        spark.stop()

