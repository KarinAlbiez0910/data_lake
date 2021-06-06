import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

config = configparser.ConfigParser()
config.read('./dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function creates a Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the song_data file from the s3 bucket
    (s3://udacity-dend/song_data) into a Spark dataframe, generate the songs_table and artists_table thereof
    and write the data back into a S3 data bucket.
    
    Arguments:
        spark: the spark session
        input_data: S3 bucket where the song_data and log_data reside
        output_data: S3 bucket where the processed tables are moved to
        
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, 
                                            title, 
                                            artist_id, 
                                            int(year), 
                                            float(duration)
                            FROM songs
                            """)

    # write songs table to parquet files partitioned by year and artist
    songs_data = os.path.join(output_data, "songs")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_data, mode="overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
                             SELECT DISTINCT artist_id, 
                                             artist_name,
                                             artist_location, 
                                             float(artist_latitude), 
                                             float(artist_longitude)
                             FROM songs
                            """)

    # write artists table to parquet files
    artists_data = os.path.join(output_data, "artists")
    artists_table.write.parquet(artists_data, mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the log_data file
    from the s3 bucket (s3://udacity-dend/log_data)
    into a Spark dataframe, generate the users_table,
    time_table and songplays_table thereof and write the data back into a S3 data bucket.
    
    Arguments:
        spark: the spark session
        input_data: S3 bucket where the song_data and log_data reside
        output_data: S3 bucket where the processed tables are moved to

    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.select("*").where(df.page == 'NextSong')

    # extract columns for users table
    df.createOrReplaceTempView("users")
    users_table = spark.sql("""
                            SELECT DISTINCT int(userId) as user_id, 
                                            firstName as first_name, 
                                            lastName as last_name, 
                                            gender, 
                                            level
                            FROM users
                            """)

    # write users table to parquet files
    users_data = os.path.join(output_data, "users")
    users_table.write.parquet(users_data, mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date', get_datetime(df.ts))

    # extract columns to create time table
    df.createOrReplaceTempView("time")
    time_table = spark.sql('''
                            SELECT DISTINCT timestamp AS start_time,
                            extract(hour from date) as hour,
                            extract(day from date) as day,
                            extract(week from date) as week,
                            extract(month from date) as month,
                            extract(year from date) as year,
                            extract(dayofweek from date) as weekday
                            FROM time
                            ''')

    # write time table to parquet files partitioned by year and month
    time_data = os.path.join(output_data, "time")
    time_table.write.partitionBy("year", "month").parquet(time_data, mode="overwrite")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")
    songplays_table = spark.sql('''
                SELECT DISTINCT
                logs.timestamp AS start_time,
                logs.userId as user_id,
                logs.level as level,
                songs.song_id as song_id,
                songs.artist_id as artist_id,
                logs.sessionId as session_id, 
                logs.location as location,
                logs.userAgent as user_agent,
                extract(month from logs.date) as month,
                extract(year from logs.date) as year
                FROM logs
                JOIN songs
                ON (logs.artist = songs.artist_name)
                ''')
    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_data = os.path.join(output_data, "songplays")
    songplays_table.write.partitionBy("year", "month").parquet(songplays_data, mode="overwrite")


def main():
    """
    Description: This function runs all the three previous functions,
    create_spark_session(), process_song_data() and
    process_log_data() and hereby creates the Spark session and
    processes both the song_data and log_data modelling the relevant tables
    and writing the tables into the S3 output bucket
    
    Arguments:
        None
        
    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifykarinalbiez"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
