"""
Spark Standalone 클러스터 기반 포트홀 탐지 파이프라인 DAG

파이프라인 흐름:
  1. check_s3_input       — S3에 raw-sensor-data 존재 확인
  2. start_cluster        — EC2 인스턴스 시작 (Master + Worker x4)
  3. start_spark          — Spark workers 파일 갱신 + start-all.sh
  4. download_code        — S3 → Spark Master /tmp/spark-job/ 코드 다운로드
  5. install_deps         — Master + Worker 전체에 pyarrow 설치
  6. run_stage1           — spark-submit: Stage1 Anomaly Detection
  7. check_s3_stage1_out  — Stage1 출력 S3 존재 확인
  8. run_stage2           — spark-submit: Stage2 Spatial Clustering
  9. check_s3_stage2_out  — Stage2 출력 S3 존재 확인
 10. load_to_rdb          — S3 Parquet → PostgreSQL 적재 (serving EC2)
 11. refresh_views        — Materialized View 갱신
 12. send_email_report    — 일일 이메일 리포트 전송
 13. stop_spark           — Spark stop-all.sh  (trigger_rule=all_done)
 14. stop_cluster         — EC2 인스턴스 중지  (trigger_rule=all_done)

환경변수 (Airflow EC2 ~/.bashrc):
  MASTER_INSTANCE_ID, WORKER1~4_INSTANCE_ID, MASTER_PRIVATE_IP, WORKER1~4_PRIVATE_IP
  AWS_REGION, S3_BUCKET, SLACK_WEBHOOK_URL (선택)
"""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup

from dag_utils import build_spark_submit_cmd, build_s3_check_cmd

# ============================================================
# 설정 — Airflow EC2의 ~/.bashrc 에 환경변수로 등록
# ============================================================
MASTER_INSTANCE_ID  = os.environ.get("MASTER_INSTANCE_ID", "")
WORKER1_INSTANCE_ID = os.environ.get("WORKER1_INSTANCE_ID", "")
WORKER2_INSTANCE_ID = os.environ.get("WORKER2_INSTANCE_ID", "")
WORKER3_INSTANCE_ID = os.environ.get("WORKER3_INSTANCE_ID", "")
WORKER4_INSTANCE_ID = os.environ.get("WORKER4_INSTANCE_ID", "")
MASTER_PRIVATE_IP   = os.environ.get("MASTER_PRIVATE_IP", "")
WORKER1_PRIVATE_IP  = os.environ.get("WORKER1_PRIVATE_IP", "")
WORKER2_PRIVATE_IP  = os.environ.get("WORKER2_PRIVATE_IP", "")
WORKER3_PRIVATE_IP  = os.environ.get("WORKER3_PRIVATE_IP", "")
WORKER4_PRIVATE_IP  = os.environ.get("WORKER4_PRIVATE_IP", "")
AWS_REGION          = os.environ.get("AWS_REGION", "ap-northeast-2")

S3_BUCKET              = os.environ.get("S3_BUCKET", "")
S3_CODE_PREFIX         = os.environ.get("S3_CODE_PREFIX", "spark-job-code")
S3_RAW_DATA_PREFIX     = os.environ.get("S3_RAW_DATA_PREFIX", "raw-sensor-data")
S3_STAGE1_OUTPUT_PREFIX = os.environ.get("S3_STAGE1_OUTPUT_PREFIX", "stage1_anomaly_detected")
S3_STAGE2_OUTPUT_PREFIX = os.environ.get("S3_STAGE2_OUTPUT_PREFIX", "stage2_spatial_clustering")

SPARK_MASTER_URI = f"spark://{MASTER_PRIVATE_IP}:7077"
JOB_DIR          = "/tmp/spark-job"
SSH_CONN_ID      = "spark_master"

# Serving EC2 (PostgreSQL + email)
SERVING_SSH_CONN_ID = "serving_server"
SERVING_DIR         = "/home/ec2-user/DE-Team3-LOV3/serving_service"
SERVING_VENV        = f"{SERVING_DIR}/venv"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

ALL_INSTANCE_IDS = (
    f"{MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} "
    f"{WORKER3_INSTANCE_ID} {WORKER4_INSTANCE_ID}"
)


# ============================================================
# Slack 알림 콜백
# ============================================================
def _send_slack(text):
    """Slack Incoming Webhook 메시지 전송"""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        req = Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urlopen(req, timeout=10)
    except Exception:
        pass


def slack_failure_callback(context):
    """Task 실패 시 Slack 알림"""
    ti = context.get("task_instance")
    exception = context.get("exception", "")
    _send_slack(
        f":red_circle: *Airflow Task 실패*\n"
        f"*DAG:* `{ti.dag_id}`\n"
        f"*Task:* `{ti.task_id}`\n"
        f"*Date:* `{context.get('execution_date')}`\n"
        f"*Error:* `{str(exception)[:300]}`"
    )


def slack_success_callback(context):
    """DAG 전체 성공 시 Slack 알림"""
    dag_run = context.get("dag_run")
    _send_slack(
        f":large_green_circle: *파이프라인 성공*\n"
        f"*DAG:* `{dag_run.dag_id}`\n"
        f"*Date:* `{dag_run.execution_date.strftime('%Y-%m-%d')}`\n"
        f"*Duration:* `{dag_run.end_date - dag_run.start_date}`"
    )


# ============================================================
# DAG 정의
# ============================================================
default_args = {
    "owner": "softeer-DE3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="pothole_pipeline_spark_standalone",
    default_args=default_args,
    description="포트홀 탐지 파이프라인: Spark 처리 → RDB 적재 → MV 갱신 → 이메일 리포트",
    schedule_interval=None,  # 수동 트리거 전용 (run_pipeline.sh)
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "pothole", "pipeline"],
    on_success_callback=slack_success_callback,
) as dag:

    # --------------------------------------------------------
    # 0. S3 입력 데이터 존재 확인
    # --------------------------------------------------------
    check_s3_input = BashOperator(
        task_id="check_s3_input",
        bash_command=build_s3_check_cmd(
            S3_BUCKET, S3_RAW_DATA_PREFIX, AWS_REGION, partition="dt={{ ds }}"
        ),
    )

    # ========================================================
    # 인프라 준비
    # ========================================================
    with TaskGroup("infra_setup", tooltip="EC2 시작 → Spark 기동 → 코드 배포 → 의존성 설치") as infra_setup:

        start_cluster = BashOperator(
            task_id="start_cluster",
            bash_command=f"""
            echo "EC2 인스턴스 시작 중..."
            aws ec2 start-instances \
                --instance-ids {ALL_INSTANCE_IDS} \
                --region {AWS_REGION}

            echo "인스턴스 running 대기 중..."
            aws ec2 wait instance-running \
                --instance-ids {ALL_INSTANCE_IDS} \
                --region {AWS_REGION}

            echo "30초 네트워크 안정화 대기..."
            sleep 30
            """,
        )

        start_spark = SSHOperator(
            task_id="start_spark",
            ssh_conn_id=SSH_CONN_ID,
            command=f"""
            source ~/.bashrc

            echo "=== Spark workers 파일 갱신 ==="
            cat > /opt/spark/conf/workers <<EOF
{WORKER1_PRIVATE_IP}
{WORKER2_PRIVATE_IP}
{WORKER3_PRIVATE_IP}
{WORKER4_PRIVATE_IP}
EOF
            echo "workers 파일 내용:"
            cat /opt/spark/conf/workers

            /opt/spark/sbin/start-all.sh
            /opt/spark/sbin/start-history-server.sh
            """,
            cmd_timeout=300,
        )

        download_code = SSHOperator(
            task_id="download_code",
            ssh_conn_id=SSH_CONN_ID,
            command=f"""
            rm -rf {JOB_DIR} && mkdir -p {JOB_DIR}
            aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage1/ {JOB_DIR}/stage1/ --recursive --region {AWS_REGION}
            aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage2/ {JOB_DIR}/stage2/ --recursive --region {AWS_REGION}
            echo "=== 다운로드 완료 ==="
            ls -la {JOB_DIR}/stage1/
            ls -la {JOB_DIR}/stage2/
            """,
            cmd_timeout=300,
        )

        install_deps = SSHOperator(
            task_id="install_deps",
            ssh_conn_id=SSH_CONN_ID,
            command=f"""
            echo "=== Master에 pyarrow 설치 ==="
            pip3 install pyarrow --quiet

            echo "=== Workers에 pyarrow 설치 ==="
            for worker in {WORKER1_PRIVATE_IP} {WORKER2_PRIVATE_IP} {WORKER3_PRIVATE_IP} {WORKER4_PRIVATE_IP}; do
                ssh -o StrictHostKeyChecking=no $worker "pip3 install pyarrow --quiet" &
            done
            wait
            echo "=== 의존성 설치 완료 ==="
            """,
            cmd_timeout=300,
        )

        start_cluster >> start_spark >> download_code >> install_deps

    # ========================================================
    # Spark 처리 (Stage1 → Stage2)
    # ========================================================
    with TaskGroup("spark_processing", tooltip="Stage1 이상탐지 → Stage2 공간클러스터링") as spark_processing:

        run_stage1 = SSHOperator(
            task_id="run_stage1",
            ssh_conn_id=SSH_CONN_ID,
            command=build_spark_submit_cmd(
                spark_master_uri=SPARK_MASTER_URI,
                job_dir=JOB_DIR,
                stage="stage1",
                main_script="stage1_anomaly_detection.py",
                py_files=["connection_stage1.py"],
                stage_args="--env stage1 --batch-date {{ ds }}",
                aws_region=AWS_REGION,
                total_executor_cores=8,
            ),
            cmd_timeout=7200,
        )

        check_s3_stage1_out = BashOperator(
            task_id="check_s3_stage1_out",
            bash_command=build_s3_check_cmd(
                S3_BUCKET, S3_STAGE1_OUTPUT_PREFIX, AWS_REGION, partition="dt={{ ds }}"
            ),
        )

        run_stage2 = SSHOperator(
            task_id="run_stage2",
            ssh_conn_id=SSH_CONN_ID,
            command=build_spark_submit_cmd(
                spark_master_uri=SPARK_MASTER_URI,
                job_dir=JOB_DIR,
                stage="stage2",
                main_script="stage2_spatial_clustering.py",
                py_files=["connection_stage2.py"],
                stage_args="--env stage2 --batch-date {{ ds }}",
                aws_region=AWS_REGION,
                total_executor_cores=8,
            ),
            cmd_timeout=3600,
        )

        check_s3_stage2_out = BashOperator(
            task_id="check_s3_stage2_out",
            bash_command=build_s3_check_cmd(
                S3_BUCKET, S3_STAGE2_OUTPUT_PREFIX, AWS_REGION, partition="dt={{ ds }}"
            ),
        )

        run_stage1 >> check_s3_stage1_out >> run_stage2 >> check_s3_stage2_out

    # ========================================================
    # 서빙 (RDB 적재 → MV 갱신 → 이메일)
    # ========================================================
    with TaskGroup("serving", tooltip="PostgreSQL 적재 → MV 갱신 → 이메일 리포트") as serving:

        load_to_rdb = SSHOperator(
            task_id="load_to_rdb",
            ssh_conn_id=SERVING_SSH_CONN_ID,
            command=f"""
            set -a && source {SERVING_DIR}/.env && set +a
            source {SERVING_VENV}/bin/activate
            cd {SERVING_DIR}
            python -m loaders.pothole_loader \
                --s3-path "s3://{S3_BUCKET}/{S3_STAGE2_OUTPUT_PREFIX}/dt={{{{ ds }}}}/" \
                --date "{{{{ ds }}}}" \
                --config config.yaml
            """,
            cmd_timeout=600,
        )

        refresh_views = SSHOperator(
            task_id="refresh_views",
            ssh_conn_id=SERVING_SSH_CONN_ID,
            command=f"""
            set -a && source {SERVING_DIR}/.env && set +a
            docker exec pothole-postgres psql -U pothole_user -d road_safety -c "
                REFRESH MATERIALIZED VIEW CONCURRENTLY mvw_dashboard_heatmap;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mvw_dashboard_weekly_stats;
                REFRESH MATERIALIZED VIEW CONCURRENTLY mvw_dashboard_repair_priority;
            "
            echo "=== Materialized View 갱신 완료 ==="
            """,
            cmd_timeout=300,
        )

        send_email_report = SSHOperator(
            task_id="send_email_report",
            ssh_conn_id=SERVING_SSH_CONN_ID,
            command=f"""
            set -a && source {SERVING_DIR}/.env && set +a
            source {SERVING_VENV}/bin/activate
            cd {SERVING_DIR}
            python -m reporters.email_reporter \
                --email "$REPORT_EMAIL" \
                --date "{{{{ ds }}}}" \
                --config config.yaml
            """,
            cmd_timeout=120,
        )

        load_to_rdb >> refresh_views >> send_email_report

    # ========================================================
    # 인프라 정리 (성공/실패 무관)
    # ========================================================
    with TaskGroup("infra_cleanup", tooltip="Spark 종료 → EC2 중지") as infra_cleanup:

        stop_spark = SSHOperator(
            task_id="stop_spark",
            ssh_conn_id=SSH_CONN_ID,
            command="source ~/.bashrc && /opt/spark/sbin/stop-all.sh ; /opt/spark/sbin/stop-history-server.sh ;",
            cmd_timeout=120,
            trigger_rule="all_done",
        )

        stop_cluster = BashOperator(
            task_id="stop_cluster",
            bash_command=f"""
            echo "EC2 인스턴스 중지 중..."
            sleep 5
            aws ec2 stop-instances \
                --instance-ids {ALL_INSTANCE_IDS} \
                --region {AWS_REGION}
            echo "EC2 인스턴스 중지 완료 (비용 절감)"
            """,
            trigger_rule="all_done",
        )

        stop_spark >> stop_cluster

    # --------------------------------------------------------
    # 의존성
    # --------------------------------------------------------
    check_s3_input >> infra_setup >> spark_processing >> serving >> infra_cleanup
