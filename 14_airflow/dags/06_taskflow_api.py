"""
06_taskflow_api.py — TaskFlow API (@task 데코레이터)

✅ 학습 포인트:
  - Airflow 2.0+ TaskFlow API
  - @task 데코레이터로 PythonOperator 대체
  - 함수 호출 방식으로 의존성 자동 설정
  - XCom 자동 처리 (return/파라미터로 자동 전달)
  - 기존 방식 (PythonOperator) 과의 비교

💡 TaskFlow API는 Airflow 2.0에서 도입된 현대적인 DAG 작성법입니다.
   코드가 훨씬 깔끔하고 Pythonic해집니다!
"""

import random
from datetime import datetime

from airflow.decorators import dag, task


# ============================================================
# @dag 데코레이터로 DAG 정의
# ============================================================
@dag(
    dag_id="06_taskflow_api",
    description="TaskFlow API 예제 (@task 데코레이터)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["예제", "TaskFlow"],
)
def taskflow_example():
    """
    온라인 쇼핑몰 주문 처리 파이프라인

    TaskFlow API를 사용하면:
    1. @task 데코레이터로 함수를 Task로 변환
    2. 함수의 return 값이 자동으로 XCom에 저장
    3. 함수 호출 방식으로 의존성이 자동 설정
    """

    # ----------------------------------------------------------
    # Task 1: 신규 주문 수집
    # ----------------------------------------------------------
    @task()
    def fetch_orders():
        """신규 주문 데이터를 수집합니다"""
        orders = []
        products = ["맥북 프로", "아이패드", "에어팟", "애플워치", "아이폰"]
        statuses = ["pending", "confirmed", "shipped"]

        for i in range(5):
            order = {
                "order_id": f"ORD-{random.randint(1000, 9999)}",
                "product": random.choice(products),
                "quantity": random.randint(1, 3),
                "price": random.randint(100000, 3000000),
                "status": random.choice(statuses),
            }
            orders.append(order)

        print(f"📦 {len(orders)}건 주문 수집 완료")
        for o in orders:
            print(f"  [{o['order_id']}] {o['product']} x{o['quantity']} - ₩{o['price']:,}")

        # return 값이 자동으로 XCom에 저장됨!
        return orders

    # ----------------------------------------------------------
    # Task 2: 주문 처리 및 분류
    # ----------------------------------------------------------
    @task()
    def process_orders(orders: list):
        """주문을 처리하고 분류합니다"""
        # 파라미터로 이전 Task의 데이터가 자동 전달됨!
        print(f"🔄 {len(orders)}건 주문 처리 중...")

        processed = {
            "confirmed": [],
            "pending": [],
            "total_revenue": 0,
        }

        for order in orders:
            revenue = order["quantity"] * order["price"]
            processed["total_revenue"] += revenue

            if order["status"] == "pending":
                processed["pending"].append(order["order_id"])
            else:
                processed["confirmed"].append(order["order_id"])

        print(f"  ✅ 확정 주문: {len(processed['confirmed'])}건")
        print(f"  ⏳ 대기 주문: {len(processed['pending'])}건")
        print(f"  💰 총 매출: ₩{processed['total_revenue']:,}")

        return processed

    # ----------------------------------------------------------
    # Task 3: 리포트 생성
    # ----------------------------------------------------------
    @task()
    def generate_report(processed: dict):
        """처리 결과로 리포트를 생성합니다"""
        print("=" * 60)
        print("📊 주문 처리 리포트")
        print("=" * 60)
        print(f"  확정 주문 수: {len(processed['confirmed'])}건")
        print(f"    → {', '.join(processed['confirmed'])}")
        print(f"  대기 주문 수: {len(processed['pending'])}건")
        print(f"    → {', '.join(processed['pending'])}")
        print(f"  총 매출: ₩{processed['total_revenue']:,}")
        print("=" * 60)

        # 알림 메시지
        if len(processed["pending"]) > 0:
            print(f"\n⚠️ 대기 중인 주문이 {len(processed['pending'])}건 있습니다!")
        else:
            print("\n✅ 모든 주문이 처리되었습니다!")

    # ----------------------------------------------------------
    # 의존성: 함수 호출 방식으로 자동 설정!
    # ----------------------------------------------------------
    orders = fetch_orders()
    processed = process_orders(orders)        # orders의 return 값이 자동 전달
    generate_report(processed)                # processed의 return 값이 자동 전달


# DAG 인스턴스 생성
taskflow_example()
