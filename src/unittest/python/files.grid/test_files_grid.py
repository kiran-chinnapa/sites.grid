import unittest
import metadata


class TestFilesGrid(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_url = 'https://coreyms.com/portfolio/docs/Corey-Schafer-Resume.pdf'
        cls.test_file = '../../main/resources/Corey-Schafer-Resume.pdf'
        cls.test_italics_file = '../../main/resources/italics_resume.pdf'
        cls.test_underlined_file = '../../main/resources/underlined_resume.pdf'
        cls.metadata_obj = metadata.Metadata()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_file_name(self):
        self.assertEqual(self.metadata_obj.get_file_name(self.test_url),'Corey-Schafer-Resume.pdf')

    def test_get_file_description(self):
        self.assertTrue(len(self.metadata_obj.get_file_description(self.test_url))>0)

    def test_get_file_extension(self):
        self.assertEqual(self.metadata_obj.get_file_extension(self.test_url),'pdf')

    def test_get_file_creation_date(self):
        # current_date = datetime.datetime.now().strftime("%b %d %Y %H:%M:%S")
        result = self.metadata_obj.get_file_creation_date(self.test_url)
        self.assertTrue(self.metadata_obj.is_date(result))

    def test_get_file_category(self):
        self.assertEqual(self.metadata_obj.get_file_category(self.test_url), 'resume')

    def test_get_file_tech_stack(self):
        self.assertTrue(len(self.metadata_obj.get_file_tech_stack('kafka java'))>0)

    def test_get_primary_phone(self):
        self.assertTrue(len(self.metadata_obj.get_primary_phone('jkjdkfldjfkldf 3223232323 kjfldjfldf848.391.6205'))>0)

    def test_get_primary_email(self):
        self.assertTrue(
            len(self.metadata_obj.get_primary_email('jkjdkfldjfkldf 3223232323 kiranms.chinnapa@gmail.comkjfldjfldf848.391.6205')) > 0
        )

    def test_get_file_total_experience(self):
        self.assertTrue(
           self.metadata_obj.get_file_total_experience(
                'jkjdkfldjfkldf 3223232323  2021 2022 kiranms.chinnapa@gmail.comkjfldjfldf848.391.6205') > 0
        )

    def test_get_no_of_pages(self):
        self.assertTrue(self.metadata_obj.get_no_of_pages(self.test_url) > 0)
        self.assertTrue(self.metadata_obj.get_no_of_pages(self.test_file) > 0)

    def test_get_all_keywords(self):
        text = '''
        jkjdkfldjfkldf 3223232323 
        Summary 
        Education
        SUMMARY
        2021 2022 kiranms.chinnapa@gmail.comkjfldjfldf848.391.6205
        '''
        result = self.metadata_obj.get_all_keywords(text)
        print(result)
        self.assertTrue(
            len(result) > 0
        )


    def test_get_no_of_headings(self):
        self.assertTrue(
            self.metadata_obj.get_no_of_headings(self.test_url) > 0
        )

    def test_get_no_of_subheadings(self):
        self.assertTrue(
            self.metadata_obj.get_no_of_subheadings(self.test_url) > 0
        )

    def test_get_no_of_tables(self):
        self.assertTrue(
            self.metadata_obj.get_no_of_tables(self.test_url) > 0
        )

    def test_get_font_size_ratios(self):
        self.assertTrue(
            len(self.metadata_obj.get_font_size_ratios(self.test_url)) > 0
        )

    def test_get_bold_texts(self):
        result = self.metadata_obj.get_bold_italics_underlined_texts(self.test_url)
        print(result)
        self.assertTrue(
            len(result['bold']) > 0
        )

    def test_get_italics_texts(self):
        result = self.metadata_obj.get_bold_italics_underlined_texts(self.test_italics_file)
        print(result)
        self.assertTrue(
            len(result['italics']) > 0
        )

    def test_get_underlined_texts(self):
        result = self.metadata_obj.get_bold_italics_underlined_texts(self.test_underlined_file)
        print(result)
        self.assertTrue(
            len(result['underlined']) > 0
        )



    def test_get_all_capitalized_keywords(self):
        text = '''
        Everyday HOLDS 
    the 
    poSSibility 
    Of A Miracle
        '''
        result = self.metadata_obj.get_all_capitalized_keywords(text)
        print (result)
        self.assertTrue(
            len(result) > 0
        )


    def test_get_no_of_images(self):
        self.assertTrue(
            self.metadata_obj.get_no_of_images(self.test_url) > 0
        )



if __name__ == '__main__':
    unittest.main()