# Apache Airflow 실습 가이드

## 📌 Airflow란?

Apache Airflow는 **워크플로우(데이터 파이프라인)를 프로그래밍 방식으로 작성, 스케줄링, 모니터링**하는 플랫폼입니다.

### 핵심 개념

| 용어 | 설명 |
|------|------|
| **DAG** | Directed Acyclic Graph. 워크플로우 전체를 정의하는 Python 파일 |
| **Task** | DAG 안의 개별 작업 단위 |
| **Operator** | Task가 무슨 일을 할지 정의 (BashOperator, PythonOperator 등) |
| **Schedule** | DAG 실행 주기 (`@daily`, `@hourly`, cron 표현식 등) |
| **XCom** | Task 간 데이터를 주고받는 메커니즘 |
| **TaskFlow** | Airflow 2.0+ 에서 도입된 `@task` 데코레이터 기반 작성법 |

---

## 📦 Airflow 설치 방법

> ✅ **Docker 방식을 강력히 권장합니다.** OS에 관계없이 동일하게 동작합니다.

---

### 방법 1. Docker Compose (✅ 권장)

#### 사전 조건: Docker Desktop 설치

- **Windows**: https://docs.docker.com/desktop/install/windows-install/
- **macOS**: https://docs.docker.com/desktop/install/mac-install/
- **Ubuntu**:
  ```bash
  # Docker Engine 설치
  sudo apt-get update
  sudo apt-get install -y docker.io docker-compose-plugin

  # 현재 사용자를 docker 그룹에 추가 (재로그인 필요)
  sudo usermod -aG docker $USER
  ```

#### Airflow 실행

```bash
# 이 폴더(14_airflow)에서 실행
docker compose up -d

# 실행 확인 (모든 서비스가 healthy 상태인지 확인)
docker compose ps

# ⏳ 초기 실행 시 1~2분 정도 걸립니다.
```

#### Airflow 웹 UI 접속
- URL: **http://localhost:8080**
- ID: `airflow`
- PW: `airflow`

#### 종료
```bash
docker compose down

# 데이터까지 완전 삭제
docker compose down -v
```

---

### 방법 2. macOS 네이티브 설치 (pip)

```bash
# Python 가상환경 생성 (권장)
python3 -m venv airflow-venv
source airflow-venv/bin/activate

# Airflow 설치 (constraint 파일 사용)
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Airflow DB 초기화
airflow db migrate

# 관리자 계정 생성
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# 웹서버 실행 (터미널 1)
airflow webserver --port 8080

# 스케줄러 실행 (터미널 2)
airflow scheduler
```

---

### 방법 3. Ubuntu 네이티브 설치 (pip)

```bash
# 필수 패키지 설치
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

# 가상환경 생성
python3 -m venv airflow-venv
source airflow-venv/bin/activate

# Airflow 설치
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# DB 초기화 및 사용자 생성
airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# 실행 (터미널 2개)
airflow webserver --port 8080    # 터미널 1
airflow scheduler                 # 터미널 2
```

---

### 방법 4. Windows 네이티브 설치

> ⚠️ Airflow는 공식적으로 Windows를 지원하지 않습니다. **WSL2 + Ubuntu** 를 사용하세요.

```powershell
# 1. WSL2 설치 (PowerShell 관리자 모드)
wsl --install

# 2. WSL Ubuntu 터미널에서 Ubuntu 방법과 동일하게 설치
```

또는 **Docker Desktop for Windows** 설치 후 Docker 방식을 사용하세요 (✅ 권장).

---

## 📁 실습 파일 안내

| 파일 | 설명 | 학습 포인트 |
|------|------|-------------|
| `docker-compose.yml` | Airflow 실행 환경 (Docker) | Docker Compose, 서비스 구성 |
| `dags/01_hello_airflow.py` | 🟢 가장 간단한 DAG | DAG 정의, BashOperator, schedule |
| `dags/02_multi_task_pipeline.py` | 🔵 여러 Task 파이프라인 | 의존성(`>>`), 병렬 실행, PythonOperator |
| `dags/03_python_etl.py` | 🟡 Python ETL 예제 | PythonOperator, XCom, 실행날짜 |
| `dags/04_branching.py` | 🟠 조건 분기 | BranchPythonOperator, trigger_rule |
| `dags/05_xcom_example.py` | 🟣 XCom 데이터 전달 | xcom_push/pull, Task 간 통신 |
| `dags/06_taskflow_api.py` | 🔴 TaskFlow API | @task 데코레이터, 현대적 작성법 |

---

## 🚀 실습 순서

### Step 0: Airflow 실행

```bash
# 14_airflow 폴더에서 실행
docker compose up -d

# 상태 확인 (모든 서비스가 healthy가 될 때까지 대기)
docker compose ps

# 웹 UI 접속: http://localhost:8080 (airflow / airflow)
```

---

### Step 1: Hello Airflow (🟢 입문)

**파일**: `dags/01_hello_airflow.py`

1. 웹 UI에서 `01_hello_airflow` DAG를 찾습니다
2. 토글 스위치를 켜서 DAG를 활성화합니다 (Pause ↔ Unpause)
3. ▶️ (Trigger DAG) 버튼을 클릭하여 수동 실행합니다
4. 실행 결과를 확인합니다:
   - **Graph** 탭: Task 실행 흐름
   - **Logs** 탭: Task의 로그 확인 (Task 클릭 → Log 버튼)

---

### Step 2: 멀티 Task 파이프라인 (🔵 기본)

**파일**: `dags/02_multi_task_pipeline.py`

1. `02_multi_task_pipeline` DAG를 활성화하고 실행합니다
2. **Graph** 탭에서 병렬/순차 실행 구조를 확인합니다:
   ```
   start → [extract_users, extract_orders] → transform → load → notify
   ```
3. 각 Task를 클릭하여 로그를 확인합니다
4. `extract_users`를 일부러 실패시키면 어떻게 되는지 관찰합니다 (선택)

---

### Step 3: Python ETL (🟡 중급)

**파일**: `dags/03_python_etl.py`

1. `03_python_etl` DAG를 실행합니다
2. 각 Task 로그에서:
   - `extract`: 랜덤 매출 데이터 생성
   - `transform`: 데이터 변환 (총액, 카테고리)
   - `load`: 최종 리포트 출력
3. **Admin → XComs** 메뉴에서 Task 간 전달된 데이터를 확인합니다

---

### Step 4: 조건 분기 (🟠 중급)

**파일**: `dags/04_branching.py`

1. `04_branching` DAG를 실행합니다
2. Graph에서 실행된 분기(평일/주말)를 확인합니다
   - 실행된 Task: 초록색
   - 스킵된 Task: 분홍색
3. `trigger_rule=ONE_SUCCESS`가 왜 필요한지 이해합니다

---

### Step 5: XCom 데이터 전달 (🟣 중급)

**파일**: `dags/05_xcom_example.py`

1. `05_xcom_example` DAG를 실행합니다
2. **Admin → XComs** 에서 저장된 값들을 확인합니다:
   - `return_value`: 자동 저장된 값
   - `statistics`: 수동으로 push한 값
3. 로그에서 최종 리포트를 확인합니다

---

### Step 6: TaskFlow API (🔴 고급)

**파일**: `dags/06_taskflow_api.py`

1. `06_taskflow_api` DAG를 실행합니다
2. 기존 방식과 비교합니다:
   - **기존**: `PythonOperator` + `xcom_pull/push`
   - **TaskFlow**: `@task` 데코레이터 + 함수 호출
3. 코드가 얼마나 깔끔해지는지 `05`와 비교해봅니다

---

## 🧹 정리

```bash
# Airflow 종료
docker compose down

# 데이터(DB, 로그)까지 완전히 삭제
docker compose down -v

# 로그 폴더 삭제
rm -rf logs/
```

---

## 📚 참고 자료

- [Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Airflow 튜토리얼](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- [TaskFlow API 가이드](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
