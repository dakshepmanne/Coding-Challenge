import os
import glob
import pandas
import mysql.connector as db_connector
import logging
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    )

def get_mysql_connection(allow_dictionary=False):
    """Creates mysql connection

    Returns:
        connection: mysql database connection
        cursor: mysql cursor connection
    """
    connection = db_connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="weather"
    )
    cursor = connection.cursor(dictionary=allow_dictionary)
    return connection, cursor

def insert_data():
    """Read the files from the local and insert the data into database
    """
    query = "INSERT INTO wx_data (date, max_temp, min_temp, precipitation, station) values (%s, %s, %s, %s, %s)"
    connection, cursor = get_mysql_connection()
    files = glob.glob("..//wx_data//*.txt")
    count = 0
    for _file in files:
        data_dict = []
        station = _file.split("\\")[-1].split(".")[0]
        logging.info(f"Data is ingesting from file {station}")
        data = pandas.read_csv(_file, sep="\t")
        records = [tuple(i) + (station,) for i in data.itertuples(index=False)]
        cursor.executemany(query, records)
        connection.commit()
        count += cursor.rowcount
        logging.info(f"Data ingested successfully for file {station}")
        logging.info(f"Total number of records inserted into database so far is {count}")
    connection.close()

def data_analysis():
    """Calculates average of max_temp, average of min_temp and total precipitation
        for each year and station and stores the results into the wx_analysis database
    """
    analysis_query = """
                select YEAR(date), station, 
                avg(IF(max_temp != -9999, max_temp, null)) as avg_max_temp, 
                avg(IF(min_temp != -9999, min_temp, null)) as avg_min_temp, 
                sum(IF(precipitation != -9999, precipitation, null)) as total_precipitation
                from weather.wx_data group by YEAR(date), station;
            """
    connection, cursor = get_mysql_connection()
    cursor.execute(analysis_query)
    records = cursor.fetchall()
    insert_analysis_data_query = """
                    INSERT INTO wx_analysis 
                    (date, station, avg_max_temp, avg_min_temp, total_precipitation) 
                    values (%s, %s, %s, %s, %s)
                    """
    cursor.executemany(insert_analysis_data_query, records)
    connection.commit()
    connection.close()

def main():
    """This methods peforms following operations
        1. Read the data from the local files and insert into database
        2. Calculates statistics for wx_data tables and insert results into wx_analysis
    """
    insert_data()
    data_analysis()

if __name__ == "__main__":
    main()