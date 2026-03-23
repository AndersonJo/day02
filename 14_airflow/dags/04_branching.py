"""
04_branching.py — 조건 분기 (BranchPythonOperator)

✅ 학습 포인트:
  - BranchPythonOperator 사용법
  - 조건에 따라 다른 Task 실행
  - trigger_rule 개념 (one_success, all_done 등)
  - 요일에 따라 다른 작업 수행

파이프라인 구조:
                    ┌→ weekday_task ──┐
  check_day_of_week ─┤                 ├→ done
                    └→ weekend_task ──┘
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ============================================================
# 분기 함수
# ============================================================
def decide_branch(**context):
    """
    현재 요일에 따라 실행할 Task를 결정합니다.
    - 평일(월~금): weekday_task 실행
    - 주말(토~일): weekend_task 실행

    BranchPythonOperator는 반환값으로 실행할 task_id를 지정합니다.
    """
    execution_date = context["logical_date"]
    day_of_week = execution_date.weekday()  # 0=월, 6=일

    day_names = ["월", "화", "수", "목", "금", "토", "일"]
    print(f"📅 오늘은 {day_names[day_of_week]}요일입니다.")

    if day_of_week < 5:  # 월~금
        print("→ 평일 작업을 실행합니다.")
        return "weekday_task"
    else:
        print("→ 주말 작업을 실행합니다.")
        return "weekend_task"


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id="04_branching",
    description="조건 분기 예제 (요일별 다른 작업)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["예제", "분기"],
) as dag:

    # 1) 요일 확인 및 분기 결정
    check_day = BranchPythonOperator(
        task_id="check_day_of_week",
        python_callable=decide_branch,
    )

    # 2) 평일 작업
    weekday_task = BashOperator(
        task_id="weekday_task",
        bash_command="""
            echo "💼 평일 작업 실행!"
            echo "  - 일일 데이터 배치 처리"
            echo "  - 리포트 생성 및 전송"
            echo "  - 모니터링 알림 체크"
        """,
    )

    # 3) 주말 작업
    weekend_task = BashOperator(
        task_id="weekend_task",
        bash_command="""
            echo "🏖️ 주말 작업 실행!"
            echo "  - 주간 통계 집계"
            echo "  - 데이터 백업"
            echo "  - 로그 정리"
        """,
    )

    # 4) 마무리 (어느 분기든 실행 후 공통 실행)
    done = BashOperator(
        task_id="done",
        bash_command='echo "✅ 모든 작업 완료! $(date)"',
        # trigger_rule: 분기에서 하나만 실행되므로 one_success 필요
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 의존성 설정
    check_day >> [weekday_task, weekend_task]
    [weekday_task, weekend_task] >> done
