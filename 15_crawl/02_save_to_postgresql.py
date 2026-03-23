"""
02_save_to_postgresql.py — 허깅페이스 트렌딩 데이터를 PostgreSQL에 저장

✅ 학습 포인트:
  - psycopg2를 사용하여 PostgreSQL 연결
  - DB 구조(Schema) 명세 및 테이블 생성
  - pandas DataFrame의 데이터를 DB에 안전하게 Insert
"""

import psycopg2
from psycopg2 import sql
import pandas as pd
import sys

# ============================================================
# 설정
# ============================================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "postgres"  # 환경에 맞게 변경
DB_NAME = "huggingface_db"
TABLE_NAME = "trending_models"
CSV_FILE = "hf_trending_models.csv"

def setup_database():
    """데이터베이스를 생성합니다"""
    print("🔄 데이터베이스 접속 시도...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, 
            user=DB_USER, password=DB_PASS, dbname="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        if not cursor.fetchone():
            print(f"  ✨ '{DB_NAME}' 데이터베이스 생성 중...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        else:
            print(f"  ✨ '{DB_NAME}' 데이터베이스 확인됨.")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return False

def setup_table_and_insert(df: pd.DataFrame):
    """테이블을 생성하고 데이터를 삽입합니다"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        cursor = conn.cursor()

        # 테이블 초기화
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        
        create_table_query = f"""
        CREATE TABLE {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            model_name VARCHAR(255) NOT NULL,
            task VARCHAR(100),
            downloads VARCHAR(50),
            likes VARCHAR(50),
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)

        # 데이터 삽입
        print(f"  📦 {len(df)}개 모델 데이터 통째로 적재 시작...")
        insert_query = f"""
        INSERT INTO {TABLE_NAME} (model_name, task, downloads, likes)
        VALUES (%s, %s, %s, %s);
        """

        for _, row in df.iterrows():
            cursor.execute(
                insert_query,
                (row["model_name"], row["task"], row["downloads"], row["likes"])
            )

        conn.commit()
        print(f"  ✅ DB 저장 완료! (총 {len(df)}건)")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ 테이블 작업 실패: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("💾 크롤링 데이터를 PostgreSQL에 저장 시작")
    print("=" * 60)

    try:
        df = pd.read_csv(CSV_FILE)
        
        # 결측치 처리
        df["model_name"] = df["model_name"].fillna("Unknown")
        df["task"] = df["task"].fillna("None")
        df["downloads"] = df["downloads"].fillna("0")
        df["likes"] = df["likes"].fillna("0")

        if setup_database():
            setup_table_and_insert(df)

    except FileNotFoundError:
        print(f"❌ '{CSV_FILE}' 파일이 없습니다. 먼저 크롤러(01_huggingface_crawler.py)를 실행하세요.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
