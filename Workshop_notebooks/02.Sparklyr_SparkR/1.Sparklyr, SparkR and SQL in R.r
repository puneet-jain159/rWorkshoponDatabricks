# Databricks notebook source
# MAGIC %md
# MAGIC # Working with Spark in R
# MAGIC 
# MAGIC The `sparklyr` and `SparkR` packages provide R users access to the power of Spark.  However, for those who are new to distributed computing it can be difficult to discover how to work with data that is partitioned across a cluster.  This guide is designed to quickly ramp up R users for how to manipulate tables in Spark, and how the various APIs work with eachother. 
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC #### Table of Contents
# MAGIC 
# MAGIC 
# MAGIC * SparkSQL, `sparklyr`, and `dplyr`
# MAGIC * Reading Files
# MAGIC * Aggregations
# MAGIC * Hive Metastore and Tables: Writes and Reads
# MAGIC * New Columns with Mutate & Hive UDFs
# MAGIC <br>
# MAGIC ___
# MAGIC <br>
# MAGIC 
# MAGIC ### SparkSQL, `sparklyr`, and `dplyr`
# MAGIC 
# MAGIC Tables created with SparkSQL act as a common layer for working with data in `sparklyr`, `SparkR`, and SQL cells on Databricks.  These tables can be accessed via the _Data_ tab in the left menu bar.  Let's begin by creating a new table from a JSON file with `sparklyr`, then make our way toward working with that data in `SparkR` and SQL cells.  To learn more about this topic, see the docs [here](https://docs.databricks.com/user-guide/tables.html).
# MAGIC 
# MAGIC First load `sparklyr` and `dplyr` and connect to Spark.

# COMMAND ----------

library(sparklyr)
library(dplyr)

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Read in a JSON file from DBFS (blob storage) with `spark_read_json()`
# MAGIC 
# MAGIC This function takes a few parameters:
# MAGIC 
# MAGIC > _sc_ - Spark Connection, as defined by `spark_connect()`
# MAGIC >
# MAGIC > _name_ - Optional name to register the table for SparkSQL
# MAGIC >
# MAGIC > _path_ - The path to the JSON file in your file system
# MAGIC 
# MAGIC There are other parameters as well, which you can find in the API documentation here: https://spark.rstudio.com/reference/spark_read_json/

# COMMAND ----------

jsonDF <- spark_read_json(sc, 
                          name = 'jsonTable', 
                          path = "dbfs:/FileStore/tables/books.json")

## Take a look at our DF
head(jsonDF)

# COMMAND ----------

class(jsonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Aggregate and Analyze 
# MAGIC 
# MAGIC With `dplyr` syntax...

# COMMAND ----------

jsonDF %>% group_by(author) %>% 
  count() %>%
  arrange(desc(n))

# COMMAND ----------

# MAGIC %md
# MAGIC ...or with SQL syntax in a `%sql` cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(*) as n from jsonTable
# MAGIC GROUP BY author
# MAGIC ORDER BY n DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a Database
# MAGIC When working with tables, it can be a good idea to create your own database with a specified location in DBFS. This can be easily done using the SQL API. See the below cell. If you do not specify a location like we do below, it will be writtey by default to `/user/hive/warehouse/database.db/tablename`. When just writing a table without even specifying a database to begin with, the table will be stored in the `default` database, and its location will be in `/user/hive/warehouse/tablename`.
# MAGIC 
# MAGIC Lastly, note in `cmd 12` that we are using `tbl_change_db` to change the database that we interact with to our newly defined `tempDB`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS tempDB
# MAGIC LOCATION 'dbfs:/tmp/db/tempDB'

# COMMAND ----------

tbl_change_db(sc,"tempDB")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write a Table to Be Queried Later

# COMMAND ----------

## Write a table
## If you don't specify a database, it will write to the 'default' database, or in this case default.aggJSON
group_by(jsonDF, author) %>% 
  count() %>%
  arrange(desc(n)) %>%
  spark_write_table(name = 'persistentTable', mode = "overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from persistentTable

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Verify the table location on DBFS:

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/tmp/db/tempDB/persistenttable/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading from a Table

# COMMAND ----------

## Read a table from the db
fromTable <- spark_read_table(sc, "persistentTable") 
head(fromTable)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Type Conversions with `sparklyr`
# MAGIC 
# MAGIC `sparklyr` uses [Hive UDFs](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF) for many data transformation operations.  These are executed inside of the `mutate()` function of `dplyr`.  
# MAGIC 
# MAGIC Here's an example of creating the current date in our table, then extracting different datetime values from it.  Then we will transform the datetime value to a different format.

# COMMAND ----------

withDate <- jsonDF %>%
              mutate(current_ts = current_timestamp(),
                     month = month(current_ts),
                     year = year(current_ts),
                     current_date = date_format(current_ts, 'yyyy-MM-dd'),
                     day_of_month = dayofmonth(current_date))

## View on the columns we created
select(withDate, current_ts, current_date, day_of_month, month, year)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now let's transform the `today` column to a different date format, then extract the day of the month.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Repeat in SQL
# MAGIC 
# MAGIC We can perform the same transformations in SQL. We can use the persistent table that we created (`persistentTable`) to query from. We can either use the sparklyr command `sdf_sql` to point the result to a sparklyr dataframe, or we can just use the `%sql` magic command. Assuming we have a table registered in Spark (like `aggJSON` in this notebook), we can create new tables from that.

# COMMAND ----------

withDateSQL <- sdf_sql(sc, 
                       "select current_timestamp() as current_ts,
                               month(current_timestamp()) as month,
                               year(current_timestamp()) as year,
                               current_date() as current_date,
                               dayofmonth(current_date()) as day_of_month
                               from persistenttable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp() as current_ts,
# MAGIC                                month(current_timestamp()) as month,
# MAGIC                                year(current_timestamp()) as year,
# MAGIC                                current_date() as current_date,
# MAGIC                                dayofmonth(current_date()) as day_of_month
# MAGIC                                from persistenttable

# COMMAND ----------

# MAGIC %md
# MAGIC #### SparkR
# MAGIC Apart from Sparklyr, we can of course also use SparkR. So which one should you choose? It really is mostly down to personal preference. If you are already used to working with tidyverse, then it's probably easiest to just stick with Sparklyr, as the syntax is very similar. Sparklyr also allows for chaining operations more easily, which is not straightforward with SparkR. SparkR tends to however give slightly better performance.

# COMMAND ----------

library(magrittr)

# COMMAND ----------

sparkRjson <- SparkR::read.df("dbfs:/FileStore/tables/books.json", "json")
sparkRjson %>% SparkR::head()

# COMMAND ----------

sparkRjson %>% SparkR::groupBy(sparkRjson$author) %>% SparkR::count() %>% SparkR::head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying the same date time conversions in SparkR

# COMMAND ----------

author_counts$current_ts <- SparkR::current_timestamp()
author_counts$month <- SparkR::month(author_counts$current_ts)
author_counts$year <- SparkR::year(author_counts$current_ts)
author_counts$current_date <- SparkR::to_date(author_counts$current_ts, format='yyyy-MM-dd')
author_counts$dayofmonth <- SparkR::dayofmonth(author_counts$current_date)

final_df <- SparkR::select(author_counts, author_counts$current_ts, author_counts$current_date, author_counts$day_of_month, author_counts$month, author_counts$year)

# COMMAND ----------

# MAGIC %md
# MAGIC #### A brief note on data sources
# MAGIC So what data sources does SparkR/Sparklyr support? In this notebook we made use of a `json` file. However, SparkR/Sparklyr also supports reading from:
# MAGIC * Parquet
# MAGIC * Delta
# MAGIC * CSV/TSV/PSV
# MAGIC * orc
# MAGIC * jdbc  
# MAGIC 
# MAGIC Next, it is also easy to read from tables, using SparkR's `tableToDF` or Sparklyr's `spark_read_table` command.