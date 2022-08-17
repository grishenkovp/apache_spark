from pyspark.sql import SparkSession

spark = (SparkSession
            .builder
            .appName('streaming-kafka')
            .getOrCreate())

df = spark.read.format("parquet").load("/home/pavel/Документы/kafka/data")
df.show(25, truncate=False)