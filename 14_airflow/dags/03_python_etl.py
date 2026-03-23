"""
03_python_etl.py — PythonOperator로 실제 ETL 로직 구현

✅ 학습 포인트:
  - PythonOperator에 파라미터 전달 (op_kwargs)
  - 실행 날짜 활용 ({{ ds }}, logical_date)
  - 파일 기반 ETL (CSV 읽기 → 변환 → 저장)
  - Jinja 템플릿 사용법

이 DAG는 가상의 매출 데이터를 생성하고, 변환하고, 리포트를 출력합니다.
"""

import json
import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# ETL 함수들
# ============================================================
def extract_sales_data(**context):
    """매출 데이터를 추출 (시뮬레이션)"""
    execution_date = context["ds"]  # 실행 날짜 (YYYY-MM-DD)
    print(f"📅 실행 날짜: {execution_date}")

    # 가상 매출 데이터 생성
    products = ["노트북", "키보드", "마우스", "모니터", "헤드셋"]
    sales = []
    for i in range(10):
        sale = {
            "id": i + 1,
            "product": random.choice(products),
            "quantity": random.randint(1, 20),
            "price": random.randint(10000, 500000),
            "date": execution_date,
        }
        sales.append(sale)

    print(f"📥 {len(sales)}건 매출 데이터 추출")
    for sale in sales:
        print(f"  - {sale['product']}: {sale['quantity']}개 × ₩{sale['price']:,}")

    # 다음 Task에 데이터 전달 (XCom 사용)
    return sales


def transform_sales_data(**context):
    """매출 데이터를 변환"""
    # 이전 Task(extract)에서 XCom으로 데이터 가져오기
    ti = context["ti"]
    sales = ti.xcom_pull(task_ids="extract")

    print(f"🔄 {len(sales)}건 데이터 변환 시작")

    # 변환: 총액 계산 및 카테고리 분류
    for sale in sales:
        sale["total"] = sale["quantity"] * sale["price"]
        sale["category"] = "고가" if sale["price"] >= 200000 else "일반"

    print("✅ 변환 완료 (총액 계산, 카테고리 분류)")
    return sales


def load_report(**context):
    """변환된 데이터로 리포트 생성"""
    ti = context["ti"]
    sales = ti.xcom_pull(task_ids="transform")
    execution_date = context["ds"]

    total_revenue = sum(s["total"] for s in sales)
    total_quantity = sum(s["quantity"] for s in sales)
    premium_count = sum(1 for s in sales if s["category"] == "고가")

    print("=" * 60)
    print(f"📊 일일 매출 리포트 ({execution_date})")
    print("=" * 60)
    print(f"  총 매출액: ₩{total_revenue:,}")
    print(f"  총 판매량: {total_quantity}개")
    print(f"  고가 상품: {premium_count}건")
    print(f"  일반 상품: {len(sales) - premium_count}건")
    print("=" * 60)

    # 제품별 매출 집계
    product_totals = {}
    for s in sales:
        product_totals[s["product"]] = product_totals.get(s["product"], 0) + s["total"]

    print("\n📦 제품별 매출:")
    for product, total in sorted(product_totals.items(), key=lambda x: -x[1]):
        print(f"  {product}: ₩{total:,}")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id="03_python_etl",
    description="PythonOperator 기반 ETL 예제",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["예제", "ETL", "Python"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_sales_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_sales_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_report,
    )

    extract >> transform >> load
