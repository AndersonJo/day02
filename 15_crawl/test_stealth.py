from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    stealth = Stealth()
    # It might be page = stealth.install(page) or stealth.apply(page)? I'll just try stealth.sync_api or stealth(page)? No, stealth.apply is standard
    try:
        stealth.apply(page) # Or try to see if it doesn't crash
    except Exception:
        pass # maybe it is just stealth(page) no that failed
        
    try:
        response = page.goto("https://www.coupang.com/np/search?q=%EB%85%B8%ED%8A%B8%EB%B6%81&page=1", wait_until="domcontentloaded", timeout=15000)
        print("Status:", response.status)
        if "노트북" in page.content():
            print("SUCCESS")
        else:
            print("FAILED")
    except Exception as e:
        print("ERROR:", e)
    browser.close()
