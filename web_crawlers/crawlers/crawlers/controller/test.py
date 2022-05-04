links = [
    'https://www.apple.com/us/search',
'https://jobs.apple.com/app/en-us/profile/info',
'https://jobs.apple.com/en-us/search',
'https://www.amazon.jobs/job_categories/software-development',
'https://www.amazon.jobs/search-teamcategory',
'https://www.amazon.jobs/search-jobcategory',
'https://www.accenture.com/us-en/careers/jobsearch',
'https://www.accenture.com/us-en/search/results'
]

for link in links:
    if 0 < link.find('/job') < link.find('search'):
        print(link)
    elif link.find('/job') > 0 and link.find('/jobs') < 0:
        print(link)