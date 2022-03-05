"""SimpleApp.py"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
  .appName("1.1 - Ingest") \
  .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2")\
  .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-go02")\
  .config("spark.jars","/home/cdsw/lib/iceberg-spark3-runtime-0.9.1.1.13.317211.0-9.jar") \
  .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
  .config("spark.sql.catalog.spark_catalog.type","hive") \
  .getOrCreate()


#spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.newjar")
spark.sql("USE spark_catalog.testdb")
spark.sql("SHOW CURRENT NAMESPACE").show()
#spark.sql("DROP TABLE testtable")


spark.sql("DROP TABLE IF EXISTS testtable")
spark.sql("CREATE TABLE IF NOT EXISTS testtable (id bigint, data string, integer int) USING iceberg")

spark.sql("SELECT * FROM spark_catalog.testdb.testtable").show()

spark.read.format("iceberg").load("spark_catalog.testdb.testtable.snapshots").show(20, False)

spark.read.format("iceberg").load("spark_catalog.testdb.testtable.history").show(20, False)

spark.read.format("iceberg").load("spark_catalog.testdb.testtable.files").show(20, False)

spark.read.format("iceberg").load("spark_catalog.testdb.testtable.manifests").show(20, False)

# Insert using Iceberg format
spark.sql("INSERT INTO spark_catalog.testdb.testtable VALUES (1, 'x', 3), (2, 'y', 6), (3, 'z', 10)")

# Query using select
spark.sql("SELECT * FROM spark_catalog.testdb.testtable").show()

# Query using DF - All Data
df = spark.table("spark_catalog.testdb.testtable")
df.show(100)

from datetime import datetime

# current date and time
now = datetime.now()

timestamp = datetime.timestamp(now)
print("timestamp =", timestamp)

# Query using a point in time
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.testdb.testtable")
df.show(100)


spark.sql("DROP TABLE IF EXISTS testtableone")
df.writeTo("spark_catalog.testdb.testtableone").create()

df2=df.withColumn("integer", df.integer*3)
df2.show()

spark.sql("CREATE TABLE IF NOT EXISTS testtabletwo (id bigint, data string, integer int) USING iceberg")

#spark.sql("DROP TABLE IF EXISTS testtabletwo")
#df2.writeTo("spark_catalog.testdb.testtabletwo").create()

spark.sql("INSERT INTO testtabletwo SELECT * FROM testtableone")

df3=df2.withColumn("integer", df2.integer*3)
df3.show()

spark.sql("DROP TABLE IF EXISTS testtableupdates")
#spark.sql("CREATE TABLE IF NOT EXISTS testtablethree (id bigint, data string, integer int) USING iceberg")
df3.writeTo("spark_catalog.testdb.testtableupdates").create()

spark.sql("SELECT * FROM testtabletwo").show()

spark.sql("SELECT * FROM testtableupdates").show()

spark.sql(
"MERGE INTO testtabletwo t USING (SELECT * FROM testtableupdates) u ON t.id = u.id \
WHEN MATCHED THEN UPDATE SET t.integer = t.integer + u.integer \
WHEN NOT MATCHED THEN INSERT *")

#spark.sql("INSERT INTO testtabletwo SELECT * FROM testtabletwo")

spark.sql("SELECT * FROM testtabletwo").show()

#df.write.format("iceberg").mode("overwrite").save("testdb.testtabletwo")

#df2.writeTo("spark_catalog.testdb.testtablethree").append()