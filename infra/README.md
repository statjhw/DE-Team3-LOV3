# Infrastructure

Spark Standalone 클러스터와 Airflow 서버를 AWS EC2 위에 구성하고, 배포/운영을 자동화하는 인프라 코드입니다.

## 역할

파이프라인의 실행 환경을 코드로 관리합니다. EC2 프로비저닝 이후의 모든 설정(Spark 설치, SSH 키 배포, 설정 파일 분배, 클러스터 기동/종료)을 스크립트로 자동화하여, 새 노드를 추가하거나 클러스터를 재구성할 때 수작업 없이 재현 가능합니다.

## 인프라 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│                    VPC 10.0.0.0/16                           │
│                 Subnet 10.0.1.0/24                           │
│                                                              │
│  ┌──────────────┐    SSH    ┌──────────────────────┐         │
│  │  EC2-1       │ ────────→ │  EC2-2               │         │
│  │  Airflow     │           │  Spark Master        │         │
│  │  t3.medium   │           │  t3.large            │         │
│  │  sg_airflow  │           │  sg_spark            │         │
│  └──────────────┘           └──────┬───────────────┘         │
│                                SSH │                         │
│         ┌──────────────────────────┼────────────┐            │
│         │                          │            │            │
│         ▼                          ▼            ▼            │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────────┐  │
│  │Worker 1│ │Worker 2│ │Worker 3│ │Worker 4│ │  EC2-5     │  │
│  │t3.large│ │t3.large│ │t3.large│ │t3.large│ │  Serving   │  │
│  └────────┘ └────────┘ └────────┘ └────────┘ │  Postgres  │  │
│                                              │  Streamlit │  │
│                                              └────────────┘  │
└──────────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
   ┌──────────┐        ┌──────────┐
   │  S3      │        │  S3      │
   │  Input   │        │  Output  │
   └──────────┘        └──────────┘
```

### EC2 인스턴스 구성

| EC2 | 역할 | 인스턴스 | vCPU | 메모리 | 보안그룹 | IAM 역할 |
|-----|------|----------|------|--------|----------|----------|
| EC2-1 | Airflow 서버 | t3.medium | 2 | 4 GiB | sg_airflow | EC2FullAccess + S3ReadOnly |
| EC2-2 | Spark Master | t3.large | 2 | 8 GiB | sg_spark | S3FullAccess |
| EC2-3~6 | Spark Worker ×4 | t3.large | 2 | 8 GiB | sg_spark | S3FullAccess |
| EC2-5 | Serving (PostgreSQL + Dashboard) | - | - | - | sg_serving | S3ReadOnly |

## 핵심 설계

### Spark Standalone 선택 이유

- EC2 위에 직접 구성하면 Spark 내부 동작(리소스 할당, 셔플, S3 I/O)을 직접 튜닝 가능
- 클러스터 구성을 스크립트로 코드화하여 재현성 확보

### 중앙 집중 설정 관리 (`env.sh`)

모든 스크립트와 DAG이 참조하는 환경변수를 `env.sh` 한 파일에서 관리합니다. Worker 추가나 IP 변경 시 `env.sh`만 수정하고 `deploy_spark_configs.sh`를 재실행하면 전체 클러스터에 반영됩니다.

### Instance Profile 기반 인증

S3 접근에 Access Key 대신 **IAM Instance Profile**을 사용합니다. 키 유출 위험이 없고, `spark-defaults.conf`에서 `InstanceProfileCredentialsProvider`로 설정하여 Spark 잡이 별도 인증 없이 S3에 접근합니다.

### Worker 수평 확장

새 Worker 추가 시 코드 변경 없이 운영 절차만으로 확장됩니다.

1. 동일 스펙 EC2 생성
2. `env.sh`에 IP/Instance ID 추가
3. `setup_spark_node.sh` → `setup_ssh_keys.sh` → `deploy_spark_configs.sh` 순서로 실행
4. DAG의 `start_spark` 태스크가 workers 파일을 런타임에 갱신하므로 DAG 수정 불필요

### S3 코드 배포 (CD)

processing_service 코드를 S3에 업로드하면, DAG의 `download_code` 태스크가 실행 시마다 최신 코드를 Master로 가져옵니다. 코드 변경 시 EC2에 직접 접속할 필요가 없습니다.

배포 방법은 두 가지입니다.
- **GitHub Actions**: main 브랜치에 push 시 자동으로 S3에 업로드 (CI/CD)
- **수동**: `upload_code_to_s3.sh` 스크립트로 직접 업로드

## 구조

```
infra/
├── env.sh.template                  # 환경변수 템플릿 (IP, Instance ID, S3 등)
├── spark/
│   ├── scripts/
│   │   ├── setup_spark_node.sh      # Java 11, Spark 3.5.0, Python 패키지 설치
│   │   ├── setup_ssh_keys.sh        # Master → Workers 무비밀번호 SSH 설정
│   │   ├── deploy_spark_configs.sh  # 설정 파일 템플릿 치환 + 전체 노드 배포
│   │   ├── start_cluster.sh         # EC2 시작 → Spark 기동
│   │   └── stop_cluster.sh          # Spark 종료 → EC2 중지
│   └── spark_configs/
│       ├── spark-defaults.conf      # S3A, AQE, 리소스 설정
│       ├── spark-env.sh.master      # Master 환경변수 (Worker 메모리 3g)
│       ├── spark-env.sh.worker      # Worker 환경변수 (Worker 메모리 6g)
│       └── workers                  # Worker IP 목록 (템플릿)
├── airflow/
│   └── setup_airflow.sh             # Airflow 2.8.1 + SSH/Amazon 프로바이더 설치
└── s3_deploy/
    └── upload_code_to_s3.sh         # processing_service 코드 S3 업로드
```

## 실행 순서

```bash
# 0. 환경변수 설정
cp env.sh.template env.sh && vi env.sh

# 1. Spark 노드 초기 설정 (Master, Worker 각각)
bash spark/scripts/setup_spark_node.sh

# 2. Master → Worker SSH 키 설정 (Master에서)
bash spark/scripts/setup_ssh_keys.sh

# 3. Spark 설정 파일 배포 (Master에서)
bash spark/scripts/deploy_spark_configs.sh

# 4. Airflow 설치 (Airflow EC2에서)
bash airflow/setup_airflow.sh

# 5. 코드 S3 업로드
bash s3_deploy/upload_code_to_s3.sh
```

상세 구축 절차는 [SETUP_GUIDE.md](./SETUP_GUIDE.md)를 참조하세요.
