"""
01_huggingface_crawler.py — 허깅페이스(Hugging Face) 트렌딩 모델 크롤러

✅ 학습 포인트:
  - requests 라이브러리로 웹 페이지 가져오기
  - BeautifulSoup으로 HTML 파싱
  - 최신 AI 트렌드(모델명, 다운로드 수, 좋아요 수) 데이터 추출
  - pandas DataFrame으로 정리 및 CSV/Parquet 저장

💡 특징:
  - 전 세계 AI 모델 허브인 Hugging Face의 실시간 트렌딩 모델을 수집합니다.
  - 쿠팡과 달리 강력한 봇 차단이 없어서 빠르고 안정적으로 실습할 수 있습니다!
"""

import sys
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ============================================================
# 설정
# ============================================================
BASE_URL = "https://huggingface.co/models"
MAX_PAGES = 3  # 크롤링할 페이지 수

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def crawl_hf_trending_models(max_pages: int = 3) -> list[dict]:
    """
    Hugging Face 트렌딩 페이지에서 모델 정보를 크롤링합니다.
    """
    models = []
    
    for page in range(0, max_pages):
        print(f"\n📄 페이지 {page + 1}/{max_pages} 크롤링 중...")
        
        # sort=trending 파라미터 사용. 페이지는 p=0, p=1, ...
        url = f"{BASE_URL}?sort=trending&p={page}"
            
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status() 
            print(f"  ✅ 응답 성공!")
        except requests.RequestException as e:
            print(f"  ❌ 요청 실패: {e}")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # HTML 구조: 각 모델은 <article> 태그 안에 있습니다.
        items = soup.select("article")
        print(f"  📦 트렌딩 모델 {len(items)}개 발견")

        for item in items:
            try:
                # 1. 모델명 추출 (h4 태그 안의 텍스트)
                title_tag = item.select_one("header h4")
                if not title_tag:
                    continue
                model_name = title_tag.get_text(strip=True)

                # 2. 메타데이터 텍스트 추출 (작업 유형, 파라미터 수, 업데이트 시간, 다운로드, 좋아요)
                # 데이터 예: "Text Generation • 7B • Updated 2 days ago • 1.2M • 500"
                # 우리는 제일 뒤에 있는 다운로드 수와 좋아요 수를 가져옵니다.
                meta_text = item.get_text(separator="|", strip=True)
                
                # 원시 텍스트에서 '•' 기준으로 분리
                parts = [p.strip() for p in meta_text.split("•")]
                
                downloads = "0"
                likes = "0"
                task = "Unknown"
                
                if len(parts) >= 3:
                    likes = parts[-1].replace(",", "").replace("|", "").strip()
                    downloads = parts[-2].replace(",", "").replace("|", "").strip()
                    
                    # 작업 유형(Task)은 두 번째(인덱스 1)에 위치하는 경우가 많습니다.
                    # 첫 번째(인덱스 0)는 대부분 "모델명|모델명" 형식이므로 패스
                    if len(parts) >= 4:
                        task_part = parts[1].split("|")[-1].strip()
                        if task_part and not task_part.endswith("B"): # 파라미터 크기(7B 등)가 아닐 경우
                            task = task_part

                models.append({
                    "model_name": model_name,
                    "task": task[:50],  # 너무 길면 자르기
                    "downloads": downloads,
                    "likes": likes,
                })

            except Exception as e:
                print(f"  ⚠️ 파싱 오류: {e}")
                continue

        # 예의바른 크롤링을 위해 2초 대기
        time.sleep(2)

    return models


# ============================================================
# 메인 실행
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print(f"🤖 Hugging Face 실시간 트렌딩 모델 수집 시작")
    print("=" * 60)

    results = crawl_hf_trending_models(MAX_PAGES)
    print(f"\n✅ 총 {len(results)}개 모델 크롤링 완료!")

    if not results:
        print("❌ 크롤링 결과가 없습니다.")
        exit(1)

    # DataFrame 변환
    df = pd.DataFrame(results)
    
    # CSV 저장
    csv_path = "hf_trending_models.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 CSV 저장 완료: {csv_path}")

    # Parquet 저장 (데이터 엔지니어링 실습용)
    parquet_path = "hf_trending_models.parquet"
    if "pyarrow" in sys.modules or pd.compat._optional.IMPORT_KNOWS_ARROW:
        try:
            df.to_parquet(parquet_path, index=False)
            print(f"💾 Parquet 저장 완료: {parquet_path}")
        except:
             pass

    # 결과 출력
    print("\n" + "=" * 60)
    print("🔥 실시간 트렌딩 모델 상위 10개")
    print("=" * 60)
    
    # 터미널에 예쁘게 표출
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 50)
    print(df.head(10).to_string(index=True))
