def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    from pyspark.sql import functions as f

    spark = (SparkSession
             .builder
             .appName('streaming-kafka')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    source = (spark
              .readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', 'localhost:9092')
              .option('subscribe', 'test')
              #.option("startingOffsets", "earliest")
              .load())
    source.printSchema()

    df = source.selectExpr('CAST(value AS STRING)', 'offset')

    schema_df = StructType([ \
    StructField("city",StringType(),True), \
    StructField("manager",StringType(),True), \
    StructField("product",StringType(),True), \
    StructField("amount", IntegerType(), True) \
    ])

    df = df.select(f.from_json('value', schema_df).alias('data'))

    df_result = df.select('data.*')

    query = (df_result
         .writeStream
         .format("parquet")
         .option("path", "/home/pavel/Документы/kafka/data")
         .option("checkpointLocation", "/home/pavel/Документы/kafka/checkpointLocation")
         .outputMode("append")
         .queryName("sales")
         .trigger(processingTime='5 seconds')
        )
    query.start().awaitTermination()

if __name__ == '__main__':
    main()