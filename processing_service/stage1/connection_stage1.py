import os
import yaml
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    YAML 설정 파일 로드 (로컬 또는 s3a://)
    """
    path = config_path.strip()
    try:
        if path.startswith("s3a://"):
            from urllib.parse import urlparse
            import boto3
            parsed = urlparse(path)
            bucket, key = parsed.netloc, parsed.path.lstrip("/")
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=key)
            config = yaml.safe_load(obj["Body"].read().decode("utf-8"))
        else:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        logger.info("설정 로드: %s", config_path)
        return config or {}
    except Exception as e:
        logger.warning("설정 로드 실패 (%s): %s", config_path, e)
        return {}


def get_spark_session(config: Dict[str, Any]) -> SparkSession:
    #config의 spark 설정으로 SparkSession 생성
    spark_cfg = config.get("spark", {})
    app_name = spark_cfg.get("app_name", "Stage1-AnomalyDetection")
    master = spark_cfg.get("master")

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
        logger.info("Spark Master: %s", master)
    else:
        logger.info("Spark Master: 미설정 (클러스터 모드)")

    builder = (
        builder
        .config("spark.sql.adaptive.enabled", spark_cfg.get("adaptive_enabled", True))
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_cfg.get("adaptive_coalesce_enabled", True))
        .config("spark.sql.adaptive.skewJoin.enabled", spark_cfg.get("skew_join_enabled", True))
        .config("spark.sql.files.maxPartitionBytes", f"{spark_cfg.get('partition_size_mb', 128) * 1024 * 1024}")
        .config("spark.sql.autoBroadcastJoinThreshold", spark_cfg.get("broadcast_threshold_mb", 50) * 1024 * 1024)
    )
    shuffle = spark_cfg.get("shuffle_partitions")
    if shuffle is not None and shuffle != "auto":
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)

    spark = builder.getOrCreate()
    logger.info("SparkSession 생성 완료")
    return spark
