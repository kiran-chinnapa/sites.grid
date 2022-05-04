import docx2txt

def extract(filename):

    data = docx2txt.process(filename)

    return data

def write_data(filename, data):
    with open(filename, 'w') as f:
        f.write(data)

def main():
    docx_filename = 'word.docx'
    data = extract(docx_filename)

    txt_filename = 'data.txt'
    write_data(txt_filename, data)

if __name__ == "__main__":
    main()
