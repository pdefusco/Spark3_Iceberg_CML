import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-east-2").config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-go02").config("spark.jars","/home/cdsw/lib/iceberg-spark3-runtime-0.9.1.1.13.317211.0-9.jar").config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog").config("spark.sql.catalog.spark_catalog.type","hive").getOrCreate()
  
  
//spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.newjar")
//spark.sql("USE spark_catalog.testdb")
//spark.sql("SHOW CURRENT NAMESPACE").show()
//spark.sql("DROP TABLE testtable")

spark.sql("CREATE TABLE newtesttable (id bigint, data string) USING iceberg")


//IF NOT EXISTS
spark.sql("SELECT * FROM newtesttable")

//Insert using Iceberg format
spark.sql("INSERT INTO spark_catalog.testdb.testtable VALUES (1, 'x'), (2, 'y'), (3, 'z')")

//Query using select
spark.sql("SELECT * FROM spark_catalog.testdb.testtable").show()

//Query using DF - All Data
val df = spark.table("spark_catalog.testdb.testtable")
df.show(100)

//Insert using Iceberg format
spark.sql("INSERT INTO spark_catalog.testdb.testtable VALUES (1, 'd'), (2, 'e'), (3, 'f')")

df.writeTo("spark_catalog.testdb.testtablefour").create()

//df.write.format("iceberg").mode("overwrite").save("testdb.testtabletwo")

df.writeTo("spark_catalog.testdb.testtablefour").append()

//Query using select
spark.sql("SELECT * FROM spark_catalog.testdb.testtablefour").show()