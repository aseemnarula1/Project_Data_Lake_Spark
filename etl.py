# Importing the PySpark SQL libraries and other important packages
import configparser
import os
import glob
import zipfile
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek
from pyspark.sql.types import *

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
     
    """

    Main Module Name - etl.py

    Sub Module Name - create_spark_session

    Sub Module Name Description - This python module creates the Hadoop Spark Session for AWS:2.7.0 version.

    Input Parameters Details - N/A

    Output Parameters Details - 
    
    1.spark - This returns the spark session created for the "SparkSession" instance.
    
    """
    
    print("Starting the Spark Session")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """

    Main Module Name - etl.py

    Sub Module Name - process_song_data

    Sub Module Name Description - This python module process song log data read it from JSON file format and then write the songs_table,                                     artists_table in a parquet file format.

    Input Parameters Details - 
    
    1. spark --- This is the spark session created for the "SparkSession" instance from the python module named "create_spark_session"
    2. input_data --- This contains the input source data files directory path.
    3. output_data --- This contains the output data files directory path.

    Output Parameters Details - N/A
    
    """
    
    
    # Getting filepath to song data file
    print("Getting filepath to song data file")    
    song_data = input_data + "/song-data/song_data/A/A/*/"
    
    # Reading song data file
    print("Reading song data file")
    df = spark.read.json(song_data)

    # Extracting columns to create songs table with distinct
    print("Extracting columns to create songs table with distinct")
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
    
    # Writing songs table to parquet files partitioned by year and artist
    print("Writing songs table to parquet files partitioned by year and artist")
    songs_table.write.parquet(os.path.join(output_data, 'songs/songs.parquet'), partitionBy=['year', 'artist_id'], mode='overwrite')
    
    # Displaying the Songs Data Table rows
    songs_table.show(10, True)
    
    # Printing the Songs Data Schema
    songs_table.printSchema()

    # Extracting columns to create artists table
    print("Extracting columns to create artists table")
    artists_table =  artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # Dropping duplicates from the artists table
    print("Dropping duplicates from the artists table")
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # Printing artists table schema
    artists_table.printSchema()
    
    #Displating 5 Rows from Artists Table
    artists_table.show(5, True)
    
    # Writing artists table to parquet files
    print("Writing artists table to parquet files")
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    
    
    """

    Main Module Name - etl.py

    Sub Module Name - process_log_data

    Sub Module Name Description - This python module process log log data read it from JSON file format and then write the users_table,                                       time_table & songplays_table in a parquet file format.

    Input Parameters Details - 
    
    1. spark --- This is the spark session created for the "SparkSession" instance from the python module named "create_spark_session"
    2. input_data --- This contains the input source data files directory path.
    3. output_data --- This contains the output data files directory path.

    Output Parameters Details - N/A
    
    """
    
    # Getting filepath to log data file
    print("Getting filepath to log data file")
    log_data = input_data + "/log-data/"

    # Reading log data file
    print("Reading log data file")
    log_data_df = spark.read.json(log_data)
    
    # Filtering by actions for song plays
    print("Filtering by actions for song plays")
    log_data_df = log_data_df.where('page="NextSong"')

    # Extracting columns for users table
    print("Extracting columns for users table")
    users_table = log_data_df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # Removing duplicates from the users tables
    print("Removing duplicates from the users tables")
    users_table = users_table.drop_duplicates(subset=['userId'])
    
    # Printing the Users Table Schema
    users_table.printSchema()
    
    # Writing the users table to parquet files
    print("Writing the users table to parquet files")
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), mode='overwrite')

    # Creating timestamp column from original timestamp column with UDF lambda function
    print("Creating timestamp column from original timestamp column with UDF lambda function")
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType()) 
    
    # Adding the timestamp column into the get timestamp data frame
    print("Adding the timestamp column into the get timestamp data frame")
    log_data_df = log_data_df.withColumn("start_time", get_timestamp(log_data_df['ts'])) 
    
        
    # Adding the columns to the time table from the start_time column basically parsing the hour, day, week, month, year etc.
    print("Adding the columns to the time table from the start_time column basically parsing the hour, day, week, month, year etc.")
    log_data_df = log_data_df.withColumn("hour", hour(log_data_df['start_time']))
    log_data_df = log_data_df.withColumn("day", dayofmonth(log_data_df['start_time']))
    log_data_df = log_data_df.withColumn("week", weekofyear(log_data_df['start_time']))
    log_data_df = log_data_df.withColumn("month", month(log_data_df['start_time']))
    log_data_df = log_data_df.withColumn("year", year(log_data_df['start_time']))
    log_data_df = log_data_df.withColumn("weekday", dayofweek(log_data_df['start_time']))
    
    # Extracting the selected columns from the log data
    print("Extracting the selected columns from the log data")
    time_table = log_data_df['start_time','hour','day','week','month','year','weekday'].distinct()
    
    
    # Printing time table schema
    time_table.printSchema()
    
    # Printing the time table rows    
    time_table.show(2, False)
    
    # Writing time table to parquet files partitioned by year and month
    print("Writing time table to parquet files partitioned by year and month")
    time_table.write.parquet(os.path.join(output_data, 'time/time.parquet'), partitionBy=['year', 'month'], mode='overwrite')
    
    

    # Reading in song data to use for songplays table
    print("Reading in song data to use for songplays table")
    input_song_df = spark.read.json(input_data+'/song-data/song_data/A/*/*/*.json')

    # Extracting columns from joined song and log datasets to create songplays table 
    print("Extracting columns from joined song and log datasets to create songplays table")
    songplays_table_joined_log_table = log_data_df.join(input_song_df, 
                                   (log_data_df.song == input_song_df.title) , how='inner')
                                                        
    # Joining the joined dataframe from Songs data frame with the time table data 
    
    print("Joining the joined dataframe from Songs data frame with the time table data ")                                                        
    songplays_table_output = songplays_table_joined_log_table.alias("s").join(time_table.alias("t"), (time_table.alias("t").start_time ==                                songplays_table_joined_log_table.alias("s").start_time), how='inner')    
                                                        
    # Extracting columns from above created Songplays table output
    
    print("Extracting columns from above created Songplays table output")
    
    songplays_table = songplays_table_output.select("s.userId",  
                                                    "t.start_time","s.song_id","s.artist_id","s.level","s.sessionId",
                                                    "s.location","s.userAgent","t.year","t.month")                                                     
       
    # Writing songplays table to parquet files partitioned by year and month
    print("Writing songplays table to parquet files partitioned by year and month")
    songplays_table.write.parquet(os.path.join(output_data, 'songplays/songplays.parquet'), partitionBy=["year", "month"],mode='overwrite')
    
    # Printing the songplays table
    print("Printing the songplays table")
    songplays_table.show(5)
    
    # Printing the songplays table schema
    songplays_table.printSchema()


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/extracted_data_folder/"
    output_data = "output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
