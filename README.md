# Using Iceberg with Spark 3 in Cloudera Machine Learning

Spark 3 has been added to CML Runtimes. It introduces a variety of new features for performance optimization while the ML API (Mlib) is very similar to Spark's prior versions.

Apache Iceberg is an open table format for huge analytic datasets. Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink and Hive using a high-performance table format that works just like a SQL table. 
Iceberg has been adopted by Cloudera Data Warehouse (CDW) and Data Engineering (CDE) Data Services. You can also use it in a Spark 3 session in Cloudera Machine Learning (CML) 

Iceberg is a scalable data format for tables. 

One of Iceberg's most anticipated features is Time Travel, giving Machine Learning practictioners the ability to roll back datasets to previous states. In the context of MLOps this is a crucial component of Reproducible Machine Learning pipelines.

## Project Contents

This project provides some guidance for using Iceberg with a Spark 3 session in CML. 

The Quickstart Notebook shows how to correctly launch a Spark Session with Iceberg, create an Iceberg table, and query the data in its original state before a simple insert.

The Jobs pipeline demonastrates how to execute two simple Database Scoring scripts, simulates a failure due to a data type mistmatch, and how to automate model retraining as a result of this error.


