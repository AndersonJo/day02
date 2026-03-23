"""
05_xcom_example.py — XCom을 활용한 Task 간 데이터 전달

✅ 학습 포인트:
  - XCom (Cross-Communication) 개념
  - xcom_push / xcom_pull 사용법
  - return 값 자동 XCom 저장
  - 여러 Task에서 데이터 주고받기
  - Airflow 웹 UI에서 XCom 값 확인하기 (Admin → XComs)

⚠️ 주의: XCom은 소량의 데이터(메타데이터, 파일 경로, 카운트 등) 전달용입니다.
         대용량 데이터는 파일이나 DB를 통해 공유하세요.

파이프라인 구조:
  generate_numbers → calculate_stats → print_report
"""

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# Task 함수들
# ============================================================
def generate_random_numbers(**context):
    """랜덤 숫자 리스트를 생성하고 XCom에 저장"""
    numbers = [random.randint(1, 100) for _ in range(20)]
    print(f"🎲 생성된 숫자들: {numbers}")

    # 방법 1: return 값은 자동으로 XCom에 저장됨
    return numbers


def calculate_statistics(**context):
    """이전 Task에서 생성한 숫자들의 통계를 계산"""
    ti = context["ti"]

    # 방법 2: xcom_pull로 이전 Task의 데이터 가져오기
    numbers = ti.xcom_pull(task_ids="generate_numbers")
    print(f"📥 받은 숫자들: {numbers}")

    # 통계 계산
    stats = {
        "count": len(numbers),
        "sum": sum(numbers),
        "mean": round(sum(numbers) / len(numbers), 2),
        "min": min(numbers),
        "max": max(numbers),
    }

    print(f"📊 통계 결과: {stats}")

    # 방법 3: xcom_push로 특정 key에 데이터 저장
    ti.xcom_push(key="statistics", value=stats)
    ti.xcom_push(key="original_numbers", value=numbers)


def print_final_report(**context):
    """최종 리포트 출력"""
    ti = context["ti"]

    # key를 지정하여 특정 XCom 값 가져오기
    stats = ti.xcom_pull(task_ids="calculate_stats", key="statistics")
    numbers = ti.xcom_pull(task_ids="calculate_stats", key="original_numbers")

    print("=" * 50)
    print("📋 최종 리포트")
    print("=" * 50)
    print(f"  데이터 개수: {stats['count']}개")
    print(f"  합계: {stats['sum']}")
    print(f"  평균: {stats['mean']}")
    print(f"  최솟값: {stats['min']}")
    print(f"  최댓값: {stats['max']}")
    print(f"  범위: {stats['max'] - stats['min']}")
    print("=" * 50)

    # 50 이상인 숫자 필터링
    high_numbers = [n for n in numbers if n >= 50]
    print(f"\n  50 이상: {len(high_numbers)}개 → {high_numbers}")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id="05_xcom_example",
    description="XCom 데이터 전달 예제",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["예제", "XCom"],
) as dag:

    t1 = PythonOperator(
        task_id="generate_numbers",
        python_callable=generate_random_numbers,
    )

    t2 = PythonOperator(
        task_id="calculate_stats",
        python_callable=calculate_statistics,
    )

    t3 = PythonOperator(
        task_id="print_report",
        python_callable=print_final_report,
    )

    t1 >> t2 >> t3
