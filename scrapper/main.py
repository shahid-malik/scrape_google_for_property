from scrapper import google_search, query_city
from config import output_dir_v2, final_output_dir_v3
import os
import csv


def combine_output_files():
    o_path = os.path.join(os.getcwd(), output_dir_v2)

    column = "title,url"
    final_output_file = os.path.join(final_output_dir_v3, "output_v3.csv")
    with open(final_output_file, 'w') as fp:
        fp.writelines(column)
        fp.write("\n")

        for filename in os.listdir(o_path):
            i_file = os.path.join(o_path, filename)
            if not "_domains.csv" not in filename:
                os.remove(i_file)
            else:
                try:
                    with open(i_file, encoding="utf8", errors="ignore") as in_file:
                        data = in_file.readlines()[1:]
                        fp.writelines(data)
                except Exception as e:
                    pass


if __name__ == '__main__':
    # print(google_search("jhel"))
    google_search("Test")
    # combine_output_files()
    # query_city()

