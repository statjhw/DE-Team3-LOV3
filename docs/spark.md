# Spark 최적화 전략

---

## 1. 데이터 읽기 전략 — Small Files Problem 해결

### 문제

원본 데이터는 약 40KB짜리 파일이 5만 개로 분산 저장된 상태였습니다. S3에서 파일을 읽을 때 파일 하나당 HTTP 요청이 한 번씩 발생하므로, 파일 수가 많을수록 네트워크 오버헤드가 증가합니다. 그 결과 단순 읽기에만 약 40분이 소요되었고, 이는 전체 런타임의 99%에 해당하는 극심한 병목이었습니다.

![spark1](./images/spark1.png)

### 해결

trip 2,000개 단위의 파일들을 병합하여 24~25개의 파일로 재저장했습니다. 파일 수를 약 2,000분의 1로 줄임으로써 S3 요청 횟수가 크게 감소했고, 데이터 읽기 시간을 2분 미만으로 단축했습니다.

![spark2](./images/spark2.png)

---

## 2. 쿼리 최적화

### Broadcast Join

방지턱 CSV와 같이 크기가 작은 테이블을 조인할 때 `F.broadcast()`를 명시적으로 선언했습니다. Broadcast Join은 작은 테이블 전체를 각 워커 노드의 메모리에 복제한 뒤 로컬에서 조인을 완료하므로, 데이터를 네트워크로 재분배하는 셔플이 발생하지 않습니다.

### Window 함수 채택 (vs GroupBy + Join)

trip별 Z-score 계산을 위해 두 가지 방식을 비교 검토했습니다.

`groupBy + join` 방식은 trip_id별 통계 DataFrame을 별도로 생성한 뒤 원본 데이터와 다시 조인하는 구조로, groupBy에서 셔플 1회, join에서 셔플 1회로 총 셔플이 2회 발생합니다.
`Window.partitionBy` 방식은 같은 trip_id를 하나의 파티션에 모아 로컬에서 통계를 계산하므로 파티셔닝 과정에서 셔플이 1회만 발생합니다.

trip당 레코드 수가 400~500개 수준으로 한 파티션에 올려도 메모리 압박이 적다고 판단했고 실제 실험에서도 spill이 발생하지 않아, 셔플 횟수가 더 적은 Window 함수 방식을 최종 채택했습니다.

---

## 3. 인스턴스 타입 선정

워커 노드 인스턴스 타입을 비교 실험하여 t3.medium으로 결정했습니다.

t3.small은 vCPU 2코어에 메모리 2GB인데, executor 프로세스와 OS 오버헤드를 감안하면 실질 사용 가능한 메모리가 너무 부족하여 OOM이 발생했습니다.

t3.large는 vCPU 2코어에 메모리 8GB로, 처리 안정성은 확보되지만 현재 프로젝트의 데이터 규모에 비해 오버스펙이어서 비용 대비 효율이 낮습니다.

t3.medium은 vCPU 2코어에 메모리 4GB로, OOM 없이 안정적으로 처리되면서 현재 데이터 규모에 딱 맞는 사양으로 판단하여 채택했습니다.

---

## 4. Shuffle Partition 수 결정

`spark.sql.shuffle.partitions` 값을 실험을 통해 결정했습니다.

### shuffle.partitions = 8

파티션 수가 적어 각 태스크가 처리해야 할 데이터량이 executor 메모리(1g)를 초과했습니다. Stage 3에서는 모든 task에 걸쳐 균일하게 Spill이 발생했고, Stage 4에서도 Spill이 발생했습니다.

![spark3](./images/spark3.png)

![spark4](./images/spark4.png)
### shuffle.partitions = 16

파티션 수를 16으로 늘리자 Stage 3의 Spill이 완전히 해소되었습니다. Stage 4는 여전히 일부 Spill이 발생했지만, 실험 8에 비해 줄었습니다.

파티션 수를 더 늘리면 태스크 스케줄링 오버헤드가 커질 수 있고, 실제 수행 시간이 크게 늘어나지 않으며 task 에러도 발생하지 않아 16으로 최종 결정했습니다.

![spark5](./images/spark5.png)

![spark6](./images/spark6.png)
