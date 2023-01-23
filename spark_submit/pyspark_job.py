import sys, os
import argparse

sys.path.append("/content/apache_spark/python/lib/py4j-0.10.9.5-src.zip")
sys.path.append("/content/apache_spark/python")

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/apache_spark"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType
from pyspark.sql.functions import col,lit,array_contains
from pyspark.sql.functions import sum,avg,max,min,mean,count
from pyspark.sql.functions import udf

spark = SparkSession.builder.master('local[*]').enableHiveSupport().appName("SparkTest").getOrCreate()

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

df = spark.createDataFrame(data=data,schema=schema)

def main(employee_name):
  return df.filter(df.firstname == employee_name).show(truncate=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--employee_name', type=str, default='James', help='Please enter the name of the employee')
    args = parser.parse_args()
    name = args.employee_name
    main(name)

