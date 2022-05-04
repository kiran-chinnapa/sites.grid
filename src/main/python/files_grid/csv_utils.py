import csv
import logging


class CsvUtils:

    def add_row_to_csv(insertDataDict, index, csv_file):
        rows = insertDataDict["insert"]["rows"]
        data_file = open(csv_file, 'a')

        csv_writer = csv.writer(data_file, quoting=csv.QUOTE_ALL)

        count = 0

        for row in rows:
            if count == 0:
                # Writing headers of CSV file
                header = row.keys()
                csv_writer.writerow(header)
                count += 1

            # Writing data of CSV file
            csv_writer.writerow(row.values())
        logging.info("Success Rows Inserted:" + str(index))
        data_file.close()
