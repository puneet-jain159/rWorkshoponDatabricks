# Databricks notebook source
# Connect to Spark
SparkR::sparkR.session()
library(sparklyr)
sc <- spark_connect(method = "databricks")

# Read table in sparklyr
sparklyr_tbl <- spark_read_table(sc, "adult")

# Do stuff
transformed_tbl <- dplyr::select(sparklyr_tbl, age, workclass)

# Write to parquet
spark_write_parquet(transformed_tbl, path = "dbfs:/tmp/adult_parquet_intermediate")

# Read data in SparkR
filteredDF <- SparkR::read.df(path = "dbfs:/tmp/adult_parquet_intermediate")

# Write to Delta
SparkR::write.df(filteredDF, 
                 path = "dbfs:/home/rafi.kurlansik@databricks.com/rstudio/adult_delta",
                 source = "delta",
                 mode = "overwrite")

# Clean up temp data
system("rm -r /dbfs/tmp/adult_parquet_intermediate", intern = T)