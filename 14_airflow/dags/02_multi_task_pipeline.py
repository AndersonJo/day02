"""
02_multi_task_pipeline.py — 여러 Task로 구성된 데이터 파이프라인

✅ 학습 포인트:
  - 여러 Task 정의 및 의존성(dependency) 설정
  - >> 연산자로 실행 순서 지정
  - 병렬 실행 vs 순차 실행
  - Task 실패 시 downstream 영향

파이프라인 구조:
  start → [extract_users, extract_orders] → transform → load → notify
         (병렬 실행)                        (순차 실행)
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ============================================================
# Python 함수 (PythonOperator에서 호출됨)
# ============================================================
def transform_data():
    """추출된 데이터를 변환하는 함수 (실습용 시뮬레이션)"""
    print("=" * 50)
    print("🔄 데이터 변환 중...")
    print("  - NULL 값 제거")
    print("  - 날짜 형식 통일 (YYYY-MM-DD)")
    print("  - 중복 레코드 제거")
    print("=" * 50)


def load_data():
    """변환된 데이터를 저장하는 함수"""
    print("=" * 50)
    print("💾 데이터 적재 중...")
    print("  - 데이터베이스에 INSERT")
    print("  - 총 1,234건 적재 완료")
    print("=" * 50)


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id="02_multi_task_pipeline",
    description="여러 Task가 있는 ETL 파이프라인 예제",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["예제", "파이프라인"],
) as dag:

    # 1) 시작 알림
    start = BashOperator(
        task_id="start",
        bash_command='echo "🚀 파이프라인 시작: $(date)"',
    )

    # 2) 데이터 추출 (2개 소스에서 병렬로 추출)
    extract_users = BashOperator(
        task_id="extract_users",
        bash_command='echo "📥 사용자 데이터 추출 중..." && sleep 2 && echo "✅ 사용자 500명 추출 완료"',
    )

    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command='echo "📥 주문 데이터 추출 중..." && sleep 3 && echo "✅ 주문 1,234건 추출 완료"',
    )

    # 3) 데이터 변환 (PythonOperator)
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # 4) 데이터 적재 (PythonOperator)
    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # 5) 완료 알림
    notify = BashOperator(
        task_id="notify_completion",
        bash_command='echo "📧 파이프라인 완료 알림 전송! $(date)"',
    )

    # ============================================================
    # 의존성 설정 (실행 순서)
    # ============================================================
    # start 다음에 extract_users, extract_orders 병렬 실행
    start >> [extract_users, extract_orders]

    # 두 추출이 모두 끝난 후 transform 실행
    [extract_users, extract_orders] >> transform

    # transform → load → notify 순차 실행
    transform >> load >> notify
