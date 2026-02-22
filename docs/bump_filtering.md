### 포트홀 탐지 로직 (Impact Score 기반)

**개요**

차량이 포트홀을 통과할 때 수직 가속도(`accel_z`)와 피치 각속도(`gyro_y`)에 급격한 변화가 발생한다는 물리적 원리를 활용합니다.

두 센서값을 각각 Z-score로 정규화한 뒤 가중 합산하여 충격 점수(Impact Score) 를 산출하고, 임계값을 초과하면 포트홀 후보로 분류합니다.

**Impact Score**

impact_score= |Z accel_z| * w1 + |Z gyro_z| * w2

- `w1` : `config.impact_score.weights.accel_z`
- `w2` : `config.impact_score.weights.gyro_y`
- `threshold` : `config.impact_score.threshold`

**설계 의도**

- Z-score를 trip 단위로 계산하는 이유
    
    차량마다 장착 위치, 센서 특성, 주행 습관이 달라 절댓값 비교가 어렵습니다. trip 내 상대적 이상치를 측정함으로써 차량 간 편차를 제거하고 일관된 기준을 적용할 수 있습니다.
    

### 방지턱 필터링 로직

**개요**

포트홀 후보로 분류된 레코드가 실제 과속방지턱

위치와 일치하는 경우, 오탐(False Positive)으로 판정하여 제거합니다. [공공 방지턱 데이터](https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=TOTAL&keyword=%EB%B0%A9%EC%A7%80%ED%84%B1&detailKeyword=&publicDataPk=&recmSe=&detailText=&relatedKeyword=&commaNotInData=&commaAndData=&commaOrData=&must_not=&tabId=&dataSetCoreTf=&coreDataNm=&sort=updtDt&relRadio=&orgFullName=&orgFilter=&org=&orgSearch=&currentPage=1&perPage=10&brm=&instt=&svcType=&kwrdArray=&extsn=&coreDataNmArray=&operator=AND&pblonsipScopeCode=PBDE07)(CSV)를 활용하여 GPS 좌표 기반 근접성 검사를 수행합니다.

**근접성 판단 수식**

방지턱의 연장 길이(`bump_length`, 단위: cm)를 기준으로 허용 반경을 계산합니다.

`LENGTH_FACTOR` = 1.0 / (2.0 * 100.0 * 111000.0)

- 111000.0 : 위도 1도 = 111km(미터 변환)
- 100 : cm → m 변환
- 2 : 방지턱 연장 길이 절반을 반경으로 사용

**설계 의도**

- **Broadcast Join을 사용하는 이유**
    
    방지턱 데이터는 전국 기준으로도 수MB 수준의 소규모 테이블입니다. 이를 각 워커 노드 메모리에 복제(broadcast)하면 셔플(네트워크 데이터 교환) 없이 로컬에서 조인이 완료되어, t3.medium과 같은 저사양 클러스터 환경에서도 Spill 없이 빠르게 처리됩니다.
    
- **포트홀 후보만 조인하는 이유 (Early Filtering)**
    
    전체 데이터에 방지턱 조인을 적용하면 정상 레코드(대다수)까지 불필요한 연산이 발생합니다. 임계값 초과 레코드(소수의 의심 데이터)만 분리하여 조인함으로써 연산량을 최소화합니다.
- 