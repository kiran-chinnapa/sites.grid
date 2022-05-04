from crawlers.controller import job_postings_extractor

jp_url = "https://edcconsulting.com/careers/"
jp_obj = job_postings_extractor.JobPostingsExtractor(cp_row={'Career Page':jp_url})
print(jp_obj.is_valid_job_posting(jp_url))
jp_obj.driver.quit()

#  keywords = [r"filter|refine|position|title|posted", r"\bclear\b"]