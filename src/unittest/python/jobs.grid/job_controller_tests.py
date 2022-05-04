import unittest
import job_controller


class TestJobController(unittest.TestCase):

    def test_is_jobs_on_google(self):
        result = job_controller.find_job_posting_page('teksystems','https://www.teksystems.com/')
        self.assertIsNotNone(result)

