import time

from playwright.sync_api import Playwright, sync_playwright


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()

    # Open new page
    page = context.new_page()

    # Go to https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781
    page.goto("https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781")

    # Go to https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781&mobile=false&width=1170&height=500&bga=true&needsRedirect=false&jan1offset=330&jun1offset=330
    page.goto("https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781&mobile=false&width=1170&height=500&bga=true&needsRedirect=false&jan1offset=330&jun1offset=330")

    # Click text=view all open job positions
    # with page.expect_navigation(url="https://careers-obxtek.icims.com/jobs/search?ss=1&hashed=-435683781"):
    with page.expect_navigation():
        page.frame(name="icims_content_iframe").click("text=view all open job positions")

    # ---------------------
    page.wait_for_url()
    time.sleep(10)
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)