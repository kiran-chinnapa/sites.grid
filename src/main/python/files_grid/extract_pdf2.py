import logging
import os
import shutil
import requests
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter, resolve1
from pdfminer.converter import TextConverter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
import io
import metadata
from pdf2docx import Converter
from pdfminer.layout import LAParams

def extract(filename):

    fp = open(filename, 'rb')
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    for page in PDFPage.get_pages(fp):
        interpreter.process_page(page)
        data =  retstr.getvalue()

    return data

def extract_from_url(url):
    response = requests.get(url, timeout=5, headers={'User-Agent': "Magic Browser"})
    metadata.Metadata().req_resp_size_logger(response)
    with io.BytesIO(response.content) as file:
        remote_file = file.read()
    fp = io.BytesIO(remote_file)
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    for page in PDFPage.get_pages(fp):
        interpreter.process_page(page)
        data =  retstr.getvalue()

    return data

def get_no_of_pages_url(url):
    count = 0
    try:
        response = requests.get(url, timeout=5, headers={'User-Agent': "Magic Browser"})
        metadata.Metadata().req_resp_size_logger(response)
        with io.BytesIO(response.content) as file:
            parser = PDFParser(file)
            document = PDFDocument(parser)
        # This will give you the count of pages
            count = resolve1(document.catalog['Pages'])['Count']
    except Exception as e:
        logging.error("unable to parse file {}: {}".format(url, e))
    return count

def get_file_from_source(url_or_path):
    path = '/tmp/tmp.pdf'
    if metadata.Metadata().is_url(url_or_path):
        resp = requests.get(url_or_path, allow_redirects=True, timeout=5)
        metadata.Metadata().req_resp_size_logger(resp)
        open(path, 'wb').write(resp.content)
    else:
        shutil.copy(url_or_path, path)
    return path

def get_path_from_url(url):
    path = '/tmp/tmp.pdf'
    resp = requests.get(url, allow_redirects=True, timeout=5)
    metadata.Metadata().req_resp_size_logger(resp)
    open(path, 'wb').write(resp.content)
    return path

def convert_to_word(url_or_path):
    try:
        word_path = '/tmp/tmp.docx'
        pdf_file = get_file_from_source(url_or_path)
        # convert pdf to docx
        cv = Converter(pdf_file)
        cv.convert(word_path)  # all pages by default
        cv.close()
        os.remove(pdf_file)
        return word_path
    except Exception as e:
        logging.error("unable to convert file {}: {}".format(url_or_path, e))

def get_no_of_pages_file(file):
    count = 0
    try:
        file = open(file, 'rb')
        parser = PDFParser(file)
        document = PDFDocument(parser)
        # This will give you the count of pages
        count = resolve1(document.catalog['Pages'])['Count']
    except Exception as e:
        logging.error("unable to parse file {}: {}".format(file, e))
    return count


# def write_data(filename, data):
#     with open(filename, 'w') as f:
#         f.write(data)


def main():
    # pdf_filename = '/Users/msk/Documents/SOP_MSIS.pdf'
    pdf_file = "/Users/msk/sites_git/tests/Corey-Schafer-Resume.pdf"
    # pdf_online = 'https://coreyms.com/portfolio/docs/Corey-Schafer-Resume.pdf'
    # f = open(pdf_file, 'rb')
    # pdf_reader = PyPDF2.PdfFileReader(f)
    # page1= pdf_reader.getPage(0)
    # print(get_font_size_with_text(pdf_file))

    # docx_file = "/Users/msk/sites_git/tests/converted_resume.docx"
    # get_create_date(pdf_file)
    # print(get_no_of_pages_file(pdf_filename))
    # data = extract(pdf_filename)
    #
    # txt_filename = 'data.txt'
    # write_data(txt_filename, data)
    # print(get_no_of_pages_url(pdf_online))
    # print(extract_from_url(pdf_online))

if __name__ == "__main__":
    main()