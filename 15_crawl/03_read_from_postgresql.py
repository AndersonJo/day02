"""
03_read_from_postgresql.py — PostgreSQL에서 데이터 읽어오기

✅ 학습 포인트:
  - psycopg2를 사용하여 DB 연결
  - DB의 데이터를 pandas DataFrame으로 바로 불러오기 (read_sql)
  - 불러온 데이터 간단한 조회 및 분석
"""

import psycopg2
import pandas as pd
import sys

# ============================================================
# 설정
# ============================================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_NAME = "huggingface_db"
TABLE_NAME = "trending_models"

def read_data_from_db():
    """PostgreSQL에서 데이터를 읽어와서 DataFrame으로 반환합니다"""
    print(f"🔄 '{DB_NAME}' DB 접속 시도...")
    try:
        # 1. DB 연결
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        print("  ✅ 접속 성공!")

        # 2. SQL 쿼리 작성 (전체 데이터 조회)
        query = f"SELECT * FROM {TABLE_NAME} ORDER BY id ASC;"

        # 3. pandas를 사용하여 SQL 쿼리 결과를 바로 DataFrame으로 가져오기
        print("  📥 데이터 불러오는 중...")
        
        # pandas의 read_sql_query는 SQLAlchemy 엔진을 권장합니다.
        # sqlalchemy가 없다면 pip install sqlalchemy 로 설치하세요.
        try:
            from sqlalchemy import create_engine
            engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
            df = pd.read_sql_query(query, engine)
        except ImportError:
            # SQLAlchemy가 없을 경우를 대비한 Fallback (Warning 발생 가능)
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', UserWarning)
                df = pd.read_sql_query(query, conn)
        
        # 4. 연결 종료
        conn.close()
        
        return df

    except psycopg2.OperationalError as e:
        print(f"\n❌ DB 접속 실패: {e}")
        print("💡 PostgreSQL 서버가 켜져 있는지 확인하세요. (예: 14_airflow 의 docker compose up)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 데이터 읽기 실패: {e}")
        sys.exit(1)

# ============================================================
# 메인 실행
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("📤 PostgreSQL에서 크롤링 데이터 읽어오기")
    print("=" * 60)

    # 데이터 가져오기
    df = read_data_from_db()
    
    print(f"\n✅ 데이터 불러오기 완료! (총 {len(df)}건)")

    # 결과 표출 설정
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    print("\n" + "=" * 60)
    print("📊 불러온 데이터 확인 (상위 5건)")
    print("=" * 60)
    print(df.head(5).to_string(index=False))

    print("\n" + "=" * 60)
    print("📊 데이터 통계 확인")
    print("=" * 60)
    
    # 모델 작업(Task) 종류별 갯수 확인
    print("\n[ 작업(Task) 종류별 모델 수 ]")
    print(df['task'].value_counts())
    
    print("\n[ 최근 크롤링 시간 ]")
    latest_time = df['crawled_at'].max()
    print(f"가장 최근 크롤링된 시간: {latest_time}")
