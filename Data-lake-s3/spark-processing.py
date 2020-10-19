# --packages org.apache.hadoop:hadoop-aws:2.7.0
# import neecessary libraries


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] =  config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder  \
        .getOrCreate()

    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    hadoop_conf=spark._jsc.hadoopConfiguration()
    # see https://stackoverflow.com/questions/43454117/how-do-you-use-s3a-with-spark-2-1-0-on-aws-us-east-2
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", os.environ.get('AWS_ACCESS_KEY_ID'))
    hadoop_conf.set("fs.s3a.secret.key", os.environ.get('AWS_SECRET_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.connection.maximum", "100000")
    hadoop_conf.set("fs.s3a.endpoint", "s3." + "ap-south-1"+ ".amazonaws.com")
    return spark


def process_song_data(spark,input_data,output_data):


    song_data = input_data + "song_data/*/*/*/*"

    df = spark.read.format('json').option('mode','PERMISSIVE').\
        option('columnNameOfCorruptRecord','corrupt_record').load(song_data).drop_duplicates()

    songs_table= df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    songs_table.repartition(1).write.mode("overwrite").\
        option('partitionBy',["year","artist_id"]).\
        parquet(output_data + "songs/")

     # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()

    # write artists table to parquet files
    artists_table.repartition(1).write.mode("overwrite").parquet(output_data + "artists/")

  


def process_log_data(spark,input_data,output_data):
    """
    
    :param spark: sparksession object
    :param input_data: input data path
    :param output_data: output data path 
    """

    # get filepath 
    log_data = input_data + "log-data/"

    df = spark.read.format('json').option('mode','PERMISSIVE').\
        option('columnNameOfCorruptRecord','corrupt_record').load(log_data).drop_duplicates()


    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    users_table.repartition(1).write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.repartition(1).write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.repartition(1).drop_duplicates().write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "/home/amogh/Documents/data-engineering-workflows/Data-lake-s3/data/"
    output_data = "s3a://spark-project001/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark,input_data = input_data,output_data = output_data)


if __name__ == "__main__":
    main()







