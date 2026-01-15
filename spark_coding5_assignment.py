from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("spark_coding5_assignment")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Use your DB (create if not exists)
    spark.sql("CREATE DATABASE IF NOT EXISTS office_db")
    spark.sql("USE office_db")

    # 1) Read CSV from HDFS
    df = spark.read.option("header", True).csv("hdfs:///user/data/simple-zipcodes.csv")

    # 2) Sample 50%
    df_sample = df.sample(withReplacement=False, fraction=0.5, seed=7)

    # 3) Hive partitioned table on State and City
    #    maxRecordsPerFile = 3 (within partitions)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 3)

    # overwrite table each run (easy for assignments)
    df_sample.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("State", "City") \
        .saveAsTable("zipcodes_part")

    # 4) Run Hive SQL: states other than AL and cities other than SPRINGVILLE
    result = spark.sql("""
        SELECT RecordNumber, Country, City, Zipcode, State
        FROM zipcodes_part
        WHERE State <> 'AL' AND City <> 'SPRINGVILLE'
    """)

    result.show(truncate=False)

    # show tables (proof)
    spark.sql("SHOW TABLES IN office_db").show()

    spark.stop()

