"""
01_hello_airflow.py — 가장 간단한 Airflow DAG

✅ 학습 포인트:
  - DAG 정의 방법 (with DAG(...) 문법)
  - BashOperator 사용법
  - schedule 설정 (@daily, cron 표현식 등)
  - start_date, catchup 개념

이 DAG는 단순히 "Hello, Airflow!" 를 출력합니다.
Airflow 웹 UI (http://localhost:8080) 에서 DAG를 활성화하면 실행됩니다.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id="01_hello_airflow",              # DAG 고유 이름 (웹 UI에 표시됨)
    description="가장 간단한 Hello World DAG",
    schedule="@daily",                       # 매일 자정 실행 (@once, @hourly, cron 등 가능)
    start_date=datetime(2025, 1, 1),         # DAG 시작 날짜
    catchup=False,                           # 과거 미실행 분을 소급 실행하지 않음
    tags=["예제", "입문"],                    # 웹 UI에서 필터링용 태그
) as dag:

    # Task 1개: "Hello, Airflow!" 출력
    hello_task = BashOperator(
        task_id="say_hello",                 # Task 고유 이름
        bash_command='echo "Hello, Airflow! 🚀 현재 시각: $(date)"',
    )

    # Task가 1개뿐이므로 의존성 설정 불필요
    hello_task
