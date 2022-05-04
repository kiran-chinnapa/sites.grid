import pandas

def extract(filename):

    data = pandas.read_excel(filename)

    return data

def write_data(filename, data):
    with open(filename, 'w') as f:
        data.to_string(f, index=False, na_rep='')

def main():
    docx_filename = 'excel.xlsx'
    data = extract(docx_filename)

    txt_filename = 'data.txt'
    write_data(txt_filename, data)

if __name__ == "__main__":
    main()

