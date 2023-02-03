import json
import csv
import pandas as pd
import warnings
import os
"""
modified from origin WaybkJsonToCsvT.py

Refactor: Qihang Zhang in 2023/02/03,National University of Singapore,
NUS Business School, Department of Accounting

"""


def wayback_json_to_csv(input_json_path: str, output_csv_path: str):
    if not pd.Series(input_json_path).str.endswith("json")[0]:
        raise ValueError('Input must endswith .json')
    # read json file
    data_file = open(input_json_path)
    data = json.load(data_file)
    data_file.close()

    # start csv
    csv_file = open(output_csv_path, 'w', newline='')
    # print(output_csv_path)
    writer = csv.writer(csv_file)

    write_counter = 0
    for element in data:
        try:
            writer.writerow(element)
        except:
            warnings.warn(f"csv.writer.writerow error in {write_counter}'s element(minimum zero)")
        finally:
            write_counter += 1

    csv_file.close()


def dir_wayback_json_to_csv(input_dir, output_dir):
    """see more in https://github.com/r-boulland/Corporate-Website-Disclosure.git"""
    for filename in os.listdir(input_dir):
        if not pd.Series(filename).str.endswith(".json")[0]:
            continue
        out = os.path.join(output_dir, os.path.splitext(filename)[0])
        seq = (out, ".csv")
        out = "".join(seq)
        ticker = os.path.join(input_dir, filename)
        print('reading:', ticker)
        data_file = open(ticker)
        try:
            data = json.load(data_file)
        except:
            continue
        data_file.close()
        csv_file = open(out, 'w', newline='')
        print('writing:', out, '\n')
        writer = csv.writer(csv_file)
        for element in data:
            try:
                writer.writerow(element)
            except:
                continue
        csv_file.close()
