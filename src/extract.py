# read the csv
import csv

def extract_data(file_path):
    rows = []

    with open(file_path, mode='r',encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            rows.append(row)

    return rows 