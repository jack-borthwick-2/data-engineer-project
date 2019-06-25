import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark
import os
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import logging
from shutil import copy2
import datetime
import errno


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            logging.error(str(exc))
            raise
#credit to https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python


def write_to_raw_munged(table_name, df):
    try:
        df \
            .withColumn("year", F.lit(datetime.date.today().year)) \
            .withColumn("month", F.lit(datetime.date.today().month)) \
            .write \
            .partitionBy("year","month") \
            .mode("overwrite") \
            .parquet("munged_raw/"+table_name+"/")
    except Exception as e:
        logging.error("Error writing " + table_name + " to raw munged.  Exception: " + str(e))
        raise

def read_raw_munged(table_name):
    try:
        df = spark.read.format("parquet") \
                .load("munged_raw/" + table_name + "/*/*/*.snappy.parquet")
        return df
    except Exception as e:
        logging.error("Error loading table:" + table_name + "from raw_munged.  Eexception:" + str(e))
        raise

def write_to_results(result_name, df):
    try:
        df \
            .repartition(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "gzip") \
            .csv("api_endpoints/"+result_name+"/")
    except Exception as e:
        logging.error("Error writing results for result_name: "+ result_name + "    Exception:" + str(e))
        raise



if __name__ == "__main__":
    # execute only if run as a script

    try:
        SUBMIT_ARGS = '''
                    --master local[*]
                    --driver-memory 2g
                    --packages mysql:mysql-connector-java:5.1.46 pyspark-shell
        '''
        os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
        conf = SparkConf()
        sc = SparkContext(conf=conf)
        sql_c = SQLContext(sc)

        sc.setLogLevel("WARN")
        spark = sql_c.sparkSession
        logging.info("Spark Application is up")
    except Exception as e:
        logging.error(str(e))
        raise


    #FIRST AIRFLOW STAGE
    try: #In productioun these files would be moved using the aws Boto3 library to enable the ability to move data in S3 buckets
        mkdir_p( os.getcwd()+"/the-movies-dataset-internal/movies_metadata/year="+str(datetime.date.today().year)+"/month="+str(datetime.date.today().month))

        copy2(os.getcwd()+"/the-movies-dataset/movies_metadata.csv",  os.getcwd()+"/the-movies-dataset-internal/movies_metadata/year="+str(datetime.date.today().year)+"/month="+str(datetime.date.today().month))

        logging.info("copied raw files to internal location")
    except Exception as e:
        logging.error("error moving data into internal bucket. Exception:"+ str(e))
        raise
#NOTE Only movies_metadata is copied and munged in this scrip,t as the other table ares not necessary at this timeself.
#NOTE In production ALL OTHER tables would be copied to S3 Glacier via Boto3 as well

    #SECOND AIRFLOW STAGE

    try:
        movies_metadata = spark.read.format("csv") \
                .option("header", "true") \
                .option("infer_schema", "true") \
                .load(os.getcwd()+"/the-movies-dataset-internal/movies_metadata/year="+str(datetime.date.today().year)+"/month="+str(datetime.date.today().month)+"/movies_metadata.csv")
        genres_schema = ArrayType(
            StructType([
                StructField('id',IntegerType()),
                StructField('name',StringType())
                ])
            )
        production_companies_schema = ArrayType(
            StructType([
                StructField('id', IntegerType()),
                StructField('name', StringType())
                ])
            )
        movies_metadata = movies_metadata.withColumn("genres",
                                         F.from_json(col="genres",
                                             schema=genres_schema)
                                        )

        movies_metadata = movies_metadata.withColumn("production_companies",
                                         F.from_json(col="production_companies",
                                             schema=production_companies_schema)
                                        )

        movies_metadata = movies_metadata.withColumn("release_date", F.col("release_date").cast(DateType()))

        production_companies = movies_metadata.withColumnRenamed("id", "movie_id").select("production_companies", "movie_id") \
            .withColumn("production_companies",
                        F.explode(movies_metadata.production_companies)) \
            .select("production_companies.id",
                    "production_companies.name",
                    "movie_id") \


        genres = movies_metadata.withColumnRenamed("id", "movie_id").select("genres", "movie_id") \
            .withColumn("genres",
                        F.explode(movies_metadata.genres)) \
            .select("genres.id",
                    "genres.name",
                    "movie_id") \

        movies_metadata = movies_metadata.withColumn("profit", F.col("revenue") - F.col("budget"))

        movies_metadata = movies_metadata.distinct()
        genres = genres.distinct()
        production_companies = production_companies.distinct()


        write_to_raw_munged("production_companies", production_companies)
        write_to_raw_munged("genres", genres)
        write_to_raw_munged("movies_metadata", movies_metadata.select("popularity","id","revenue","budget","profit","release_date"))
        logging.info("Wrote tables to raw_munged")

    except Exception as e:
        logging.error("Error reading movies_metatdata.csv or writing to raw. Exception: " + str(e))
        raise

    #THIRD AIRFLOW STAGE
    try:

        genres_munged = read_raw_munged("genres")
        production_companies_munged = read_raw_munged("production_companies")
        movies_metadata_munged = read_raw_munged("movies_metadata")
        logging.info("Read tables from raw_munged")

        movies_metadata_production_companies = movies_metadata_munged.join(production_companies_munged,
                                                                           movies_metadata_munged.id == production_companies_munged.movie_id) \
                                                                            .withColumn("release_year", F.date_format(F.col("release_date"), "yyyy")) \
                                                                            .withColumnRenamed("name", "production_company_name")


        movies_metadata_genres = movies_metadata_munged.join(genres, movies_metadata_munged.id == genres.movie_id) \
                                                                            .withColumn("release_year", F.date_format(F.col("release_date"), "yyyy")) \
                                                                            .withColumnRenamed("name", "genre_name")

        movies_metadata_genres_and_production_companies = movies_metadata_genres.join(production_companies_munged,
                                                                                movies_metadata_munged.id == production_companies_munged.movie_id) \
                                                                                .withColumnRenamed("name", "production_company_name")


        revenue_by_year_and_production_company = movies_metadata_production_companies.groupby(["release_year","production_company_name"]) \
                                            .agg(F.sum(F.col("revenue"))) \
                                            .withColumnRenamed("sum(revenue)", "total_revenue")

        profit_by_year_and_production_company = movies_metadata_production_companies.groupby(["release_year","production_company_name"]) \
                                            .agg(F.sum(F.col("profit"))) \
                                            .withColumnRenamed("sum(profit)", "total_profit")

        budget_by_year_and_production_company = movies_metadata_production_companies.groupby(["release_year","production_company_name"]) \
                                            .agg(F.sum(F.col("budget"))) \
                                            .withColumnRenamed("sum(budget)", "total_budget")

        average_popularity_by_year_and_production_company = movies_metadata_production_companies.groupby(["release_year","production_company_name"]) \
                                            .agg(F.mean(F.col("popularity"))) \
                                            .withColumnRenamed("avg(popularity)", "average_popularity")

        releases_by_year_and_production_company_and_genre = movies_metadata_genres_and_production_companies.groupby("release_year", "genre_name", "production_company_name") \
                                                        .agg(F.count("release_year")) \
                                                        .withColumnRenamed("count(release_year)", "movie_count")

        w = Window().partitionBy("release_year") \
                    .orderBy(F.col("avg(popularity)") \
                            .desc())

        most_popular_genre_by_year = movies_metadata_genres.groupby(["release_year", "genre_name"]) \
                                .agg(F.mean("popularity")) \
                                .withColumn("rank", F.rank().over(w)) \
                                .filter(F.col("rank") == 1) \
                                .select("release_year", "genre_name", "avg(popularity)") \
                                .withColumnRenamed("avg(popularity)", "average_popularity")

        budget_by_year_and_genre = movies_metadata_genres.groupby(["release_year", "genre_name"]) \
                                .agg(F.sum("budget")) \
                                .withColumnRenamed("sum(budget)", "total_budget")

        revenue_by_year_and_genre = movies_metadata_genres.groupby(["release_year", "genre_name"]) \
                                .agg(F.sum("revenue")) \
                                .withColumnRenamed("sum(revenue)", "total_revenue")

        profit_by_year_and_genre = movies_metadata_genres.groupby(["release_year", "genre_name"]) \
                                .agg(F.sum("profit")) \
                                .withColumnRenamed("sum(profit)", "total_profit")

        write_to_results("budget_by_year_and_production_company", budget_by_year_and_production_company)
        write_to_results("revenue_by_year_and_production_company", revenue_by_year_and_production_company)
        write_to_results("profit_by_year_and_production_company", profit_by_year_and_production_company)
        write_to_results("releases_by_year_and_production_company_and_genre", releases_by_year_and_production_company_and_genre)
        write_to_results("average_popularity_by_year_and_production_company", average_popularity_by_year_and_production_company)
        write_to_results("most_popular_genre_by_year", most_popular_genre_by_year)
        write_to_results("average_popularity_by_year_and_production_company", average_popularity_by_year_and_production_company)
        write_to_results("budget_by_year_and_genre", budget_by_year_and_genre)
        write_to_results("revenue_by_year_and_genre", revenue_by_year_and_genre)
        write_to_results("profit_by_year_and_genre", profit_by_year_and_genre)
        logging.info("Wrote results to api_endpoints")

    except Exception as e:
        logging.error("Error computing or writing results. Expcetion: " + str(e))
        raise
