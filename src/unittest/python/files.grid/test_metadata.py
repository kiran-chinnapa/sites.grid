# learning exercise from https://www.youtube.com/watch?v=6tNS--WetLI
# Things learnt
# 1.  Extending unittest.TestCase module for writing unit tests.
# 2. Name of the test class and method should start with "test" otherwise the python execution will ignore them.None
# 3. You can run all the tests within the file from command line by calling unittest.main() within the main method.
# 4. You can execute steps before each test by using setup method/after each test by using teardown method.
# 5. You can execute steps before all tests within the file using setupClass method/after all tests in file using tearDownClass method.
# 6. Mocking is also possible within python.
# **************************Best practices*****************************
# 1.  Tests should be isolated, it should not rely on other tests, should be able to run idependently.
# 2.  Test driven development, write the test first by keeping the code which your going to test against in mind.
# So when your run the test it will fail because code is not there

import unittest
import metadata
import regex_all


class TestMetadata(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('setupClass')

    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')


    # runs before every single test
    def setUp(self):
        print('setUp')
        self.metadata_obj = metadata.Metadata(regex_all.resume)

    # runs after every single test
    def tearDown(self):
        print('tearDown')
        pass

    def test_contains_resume(self):
        print('test_contains_resume')
        text = ''' 
        SUMMARY
        dude cirriculum vitae 
        '''
        text1 ='''
    2116 haven rd apt d wilmington de 19809
         bro   EDUCATION    bro   
        bro        EDUCATION bro
            SUMMARY       
                SUMMARY
    '''
        self.assertTrue(self.metadata_obj.contains_resume(text))
        self.assertTrue(self.metadata_obj.contains_resume(text1))

    def test_contains_resume1(self):
        print('test_contains_resume1')
        text = ''' 
        EDUCATION
           dude cirriculum vitae 
           '''
        text1 = '''
       2116 haven rd apt d wilmington de 19809
            bro   EDUCATION    bro   
           bro        EDUCATION bro
               SUMMARY       
                   SUMMARY
       '''
        self.assertTrue(self.metadata_obj.contains_resume(text))
        self.assertTrue(self.metadata_obj.contains_resume(text1))


if __name__ == '__main__':
    unittest.main()