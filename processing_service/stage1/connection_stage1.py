import os
import yaml
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
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
        .config("spark.sql.adaptive.enabled", spark_cfg.get("adaptive_enabled", False))
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_cfg.get("adaptive_coalesce_enabled", False))
        .config("spark.sql.adaptive.skewJoin.enabled", spark_cfg.get("skew_join_enabled", False))
        .config("spark.sql.files.maxPartitionBytes", f"{spark_cfg.get('partition_size_mb', 128) * 1024 * 1024}")
    )
    shuffle = spark_cfg.get("shuffle_partitions")
    if shuffle is not None and shuffle != "auto":
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)

    spark = builder.getOrCreate()

    conf = spark.sparkContext.getConf()
    logger.info("executor-memory    : %s", conf.get("spark.executor.memory", "미설정"))
    logger.info("executor-cores     : %s", conf.get("spark.executor.cores", "미설정"))
    logger.info("num-executors      : %s", conf.get("spark.executor.instances", "미설정"))
    logger.info("driver-memory      : %s", conf.get("spark.driver.memory", "미설정"))
    logger.info("shuffle.partitions : %s", conf.get("spark.sql.shuffle.partitions", "미설정"))
    logger.info("adaptive.enabled   : %s", conf.get("spark.sql.adaptive.enabled", "미설정"))
    logger.info("maxPartitionBytes  : %s", conf.get("spark.sql.files.maxPartitionBytes", "미설정"))

    logger.info("SparkSession 생성 완료")
    return spark
