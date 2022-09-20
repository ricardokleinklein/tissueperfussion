""" Local Database Server

Tools devoted to the setup of a local database from the set of CSV files
that conform the AmsterdamUMCdb database using in-built sqlite3 and pandas.

When called, this script builds in the same directory a sqlite version of
the database from the official zip file downloaded (~8.5 GB - no need to
unzip since numericitems alone takes more than 80 GB).

Author: Ricardo Kleinlein
Contact: ricardokleinklein@gmail.com
Date: 09/15/2022

TODO:
    - Clean up data reading
    - Docstrings

"""
import pandas
import sqlite3

from sqlite3 import Error
from tqdm import tqdm
from zipfile import ZipFile


ZIP = '/mnt/HDD/DATA/AmsterdamUMCdb/AmsterdamUMCdb-v1.0.2.zip'


def read_file_from_zip(zip_path, filename):
    with ZipFile(zip_path, 'r') as zipf:
        try:
            with zipf.open(filename + '.csv') as zipg:
                data = pandas.read_csv(zipg)
        except UnicodeDecodeError:
            with zipf.open(filename + '.csv') as zipg:
                colnames = zipg.readline().decode('UTF-8').split(',')
                colnames[-1] = colnames[-1].rstrip()
                data = pandas.read_csv(zipg, encoding="latin1", header=None, names=colnames)
    return data


class SQLConnection:

    def __init__(self, db_path: str) -> None:
        self.connection = None
        try:
            self.connection = sqlite3.connect(db_path)
            print("Connection to SQLite DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")

    def execute_query(self, query: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occurred")

    def save_to_database(self, dataname: str, data: pandas.DataFrame) -> None:
        data.to_sql(dataname, self.connection, if_exists='replace',
                    index=False)

    def save_to_database_large(self, dataname: str, zip_path: str) -> None:
        with ZipFile(zip_path, 'r') as zipf:
            try:
                with zipf.open(dataname + '.csv') as zipg:
                    data = pandas.read_csv(zipg)
            except UnicodeDecodeError:
                with zipf.open(dataname + '.csv') as zipg:
                    colnames = zipg.readline().decode('UTF-8').split(',')
                    colnames[-1] = colnames[-1].rstrip()
                    for chunk in tqdm(pandas.read_csv(zipg, encoding="latin1", names=colnames, chunksize=1024)):
                        chunk.to_sql(dataname, self.connection, if_exists='append', index=False)

    def execute_read_query(self, query: str):
        return pandas.read_sql(query, self.connection)

    def close(self) -> None:
        self.connection.close()
        print('Connection to SQLite DB closed')


def main():
    filenames = ['admissions', 'drugitems', 'freetextitems', 'listitems',
                 'procedureorderitems', 'processitems', 'numericitems']
    sql_server = SQLConnection('./amsterdamumcdb.db')
    for file in filenames:
        print(f'Importing {file}')
        if file != 'numericitems':
            data = read_file_from_zip(ZIP, file)
            sql_server.save_to_database(file, data)
        else:
            sql_server.save_to_database_large(file, ZIP)
    sql_server.close()


if __name__ == "__main__":
    main()
