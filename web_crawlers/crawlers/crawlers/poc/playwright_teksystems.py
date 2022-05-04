import time

from playwright.sync_api import Playwright, sync_playwright


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()

    # Open new page
    page = context.new_page()

    # Go to https://careers.teksystems.com/us/en
    page.goto("https://careers.teksystems.com/us/en")

    # Click [placeholder="Search by Job Title, Keywords"]
    page.click("[placeholder=\"Search by Job Title, Keywords\"]")

    # Fill [placeholder="Search by Job Title, Keywords"]
    page.fill("[placeholder=\"Search by Job Title, Keywords\"]", "Java Developer")

    # Click [placeholder="Enter City, State or Zip"]
    page.click("[placeholder=\"Enter City, State or Zip\"]")

    # Fill [placeholder="Enter City, State or Zip"]
    page.fill("[placeholder=\"Enter City, State or Zip\"]", "USA")

    # Click button:has-text("Search")
    page.click("button:has-text(\"Search\")")
    # assert page.url == "https://careers.teksystems.com/us/en/search-results?keywords=Java%20Developer&p=ChIJCzYy5IS16lQRQrfeQ5K5Oxw&location=USA"

    # Click text=Senior Java Developer
    page.click("text=Senior Java Developer")
    # assert page.url == "https://careers.teksystems.com/us/en/job/JP-002462380/Senior-Java-Developer"

    time.sleep(10)

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
