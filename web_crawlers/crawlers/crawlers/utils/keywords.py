JOB_TITLE = r"job title|posted|find jobs"
# JOBS_PAGE = r"(?=.*career)(?=.*us)"
CAREER_PAGE_OLD = r"careers/|/careers|careers/.|jobs/|/jobs/"
# below regex needs testing before use.
CAREER_PAGE = r"careers/|/career|jobs/|/find|/job|/open|/opportunities|.jobs"
CAREER_PAGE_DENY = r"-"
JOBS_PAGES = r"job|career|recruitment|openings"
RESUME = r"(summary|experience|education|projects|skills|hobbies|portfolio|resume|curriculum|vitae)"
TECH_STACK = r"(react|svelte|java|python|kafka|snowflake|postgres|jenkins|gitlab|nexus|pcf|terraform|kubernetes|aws|azure|gcp|linux)"
JOB_DESC = r"(summary|experience|education|projects|skills|react|svelte|java|python|kafka|snowflake|postgres|jenkins|gitlab|nexus|pcf|terraform|kubernetes|aws|azure|gcp|linux)"
JOB_TITLES = r'engineer$|developer$|programmer$|architect$|engineering$|analyst$|coder$|designer$|specialist$|manager$|operator$|coordinator$'
# def myFun(*args, **kwargs):
#     print("args: ", args)
#     print("kwargs: ", kwargs)
#
#
# myFun(bro='dude', first="Geeks", mid="for", last="Geeks")
