import bs4
import requests

job_url = 'https://careers.teksystems.com/us/en/job/JP-002392804/Java-Developer'
resp  = requests.get(job_url)
if resp.status_code == 200:
    # print(resp.text)
    soup = bs4.BeautifulSoup(resp.text.lower(),'lxml')
    print(soup.find_all(text=True))

# Name
# Description
# Minimum qualification
# Preferred qualification
# Salary
# Full Time or Contract
# Contract Period
# Skills needed
# Company Id
# Benefits
# Vacation policy
# Certifications required
# Linguistic Language required