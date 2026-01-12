from playwright.sync_api import sync_playwright

def fetch_visible_elements():
    url = "https://cordis.europa.eu/search?q=contenttype%3D%27project%27%20AND%20frameworkProgramme%3D%27HORIZON%27&p=1&num=10&srt=/project/contentUpdateDate:decreasing"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"[INFO] Navigating to: {url}")
        page.goto(url, timeout=60000)
        page.wait_for_timeout(10000)

        visible_tags = page.query_selector_all("*")

        print("\n[INFO] === TEXT FROM VISIBLE ELEMENTS ===\n")
        count = 0
        for el in visible_tags:
            try:
                text = el.inner_text().strip()
                if text:
                    print(f"- {text}")
                    count += 1
                    if count >= 20:
                        break
            except:
                continue

        print("\n[INFO] === DONE ===\n")
        browser.close()

if __name__ == "__main__":
    fetch_visible_elements()