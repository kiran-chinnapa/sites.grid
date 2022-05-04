import textract

def extract(filename):

    data = textract.process(filename).decode()

    return data

def write_data(filename, data):
    with open(filename, 'w') as f:
        f.write(data)

def main():
    docx_filename = 'file-sample_100kB.doc'
    data = extract(docx_filename)

    txt_filename = 'data.txt'
    write_data(txt_filename, data)

main()
