from crawlers.utils import grid_utils


def calculate_jp_sr():
    cp_career_pages = grid_utils.get_grid_rows(grid_utils.qa_career_grid_id)
    jp_career_pages = grid_utils.get_distinct_rows('Career Page', grid_utils.qa_job_posting_page_grid)

    count = 0
    cp_len = len(cp_career_pages)
    jp_len = len(jp_career_pages)
    cp_set = set()
    if cp_len == jp_len:
        print("******** Job Posting Success rate {}% ************".format(100))
    else:
        with open('../resources/uncrawled.csv', 'w') as f:
            f.write('"Career Page","Company Website","Company Name","DMV Grid Name","DMV Grid Id"\n')
            for cp_career_page in cp_career_pages:
                if cp_career_page['Career Page'] in cp_set:
                    print("duplicate cp {}".format(cp_career_page['Career Page']))
                else:
                    cp_set.add(cp_career_page['Career Page'])
                if cp_career_page['Career Page'] not in jp_career_pages:
                    print("{} Career Page has no job postings".format(cp_career_page))
                    f.write('"{}","{}","{}","{}","{}"\n'.format(cp_career_page['Career Page'],
                                                                cp_career_page['Company Website'],
                                                                cp_career_page['Company Name'],
                                                                cp_career_page['DMV Grid Name'],
                                                                cp_career_page['DMV Grid Id']))
                    count = count + 1
        rate = '{0:.2f}'.format((100 * (cp_len - count)) / cp_len)
        print("Total Career Pages:{}".format(cp_len))
        print("Career Pages having valid Job Postings Page:{}".format(cp_len - count))
        print("Percentage of Career Pages having Job Postings:{}".format(rate))
        print("Career Pages having no Job Postings Page:{}".format(count))


def calculate_jobs_sr():
    job_posting_pages = grid_utils.get_grid_rows(grid_utils.qa_job_posting_page_grid)
    jobs_jp_pages = grid_utils.get_distinct_rows('Job Posting Page', grid_utils.qa_jobs_grid_id)

    count = 0
    jps_len = len(job_posting_pages)
    jobs_len = len(jobs_jp_pages)
    jp_set = set()
    if jps_len == jobs_len:
        print("******** Job Posting Success rate {}% ************".format(100))
    else:
        with open('../resources/uncrawled.csv', 'w') as f:
            f.write('"Job Posting Page","Career Page","Company Website","Company Name","DMV Grid Name","DMV Grid Id"\n')
            for job_posting_page in job_posting_pages:
                if job_posting_page['Job Posting Page'] in jp_set:
                    print("duplicate cp {}".format(job_posting_page['Job Posting Page']))
                else:
                    jp_set.add(job_posting_page['Job Posting Page'])
                if job_posting_page['Job Posting Page'] not in jobs_jp_pages:
                    print("{} Job Posting Page is not crawled".format(job_posting_page))
                    f.write('"{}","{}","{}","{}","{}"\n'.format(job_posting_page['Job Posting Page'],
                                                                job_posting_page['Career Page'],
                                                                job_posting_page['Company Website'],
                                                                job_posting_page['Company Name'],
                                                                job_posting_page['DMV Grid Name'],
                                                                job_posting_page['DMV Grid Id']))
                    count = count + 1
        rate = '{0:.2f}'.format((100 * (jps_len - count)) / jps_len)
        print("Total Job Posting Pages:{}".format(jps_len))
        print("Job Posting Pages having valid Jobs:{}".format(jps_len - count))
        print("Percentage of Job Posting Pages having Jobs:{}".format(rate))
        print("Job Posting Pages having no Jobs:{}".format(count))


if __name__ == '__main__':
    type = input('which success rate do you want to calculate ? allowed (job_postings, jobs)\n')
    if 'job_postings' in type:
        calculate_jp_sr()
    if 'jobs' in type:
        calculate_jobs_sr()
    else:
        print('invalid in put')
