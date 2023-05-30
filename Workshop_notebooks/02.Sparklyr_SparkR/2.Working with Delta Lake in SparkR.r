# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ensuring Consistency with ACID Transactions with Delta Lake and SparkR 
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC This is a companion notebook to provide a Delta Lake example against the Lending Club data.
# MAGIC * This notebook has been tested with *DBR 6.2, Python 3, SparkR 2.4.5*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake
# MAGIC 
# MAGIC Optimization Layer a top blob storage for Reliability (i.e. ACID compliance) and Low Latency of Streaming + Batch data pipelines.

# COMMAND ----------

# MAGIC %md ## Import Data and create pre-Delta Lake Table
# MAGIC * This will create a lot of small Parquet files emulating the typical small file problem that occurs with streaming or highly transactional data

# COMMAND ----------

library(SparkR)
library(magrittr)

# Configure location of loanstats_2012_2017.parquet
lspq_path <- "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
data <- read.parquet(lspq_path)

# Reduce the amount of data (to run on DBCE)
loan_stats <- randomSplit(
  data,
  c(0.02, 0.98),
  seed = 206
)[[1]]

# Select only the columns needed, aggregate count by state
loan_by_state <- loan_stats %>% 
    select("addr_state", "loan_status") %>%
    groupBy("addr_state") %>%
    count()

# Register a table with SparkSQL
createOrReplaceTempView(loan_by_state, "loan_by_state")

# Display loans by state
display(loan_by_state)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Easily Convert Parquet to Delta Lake format
# MAGIC With Delta Lake, you can easily transform your Parquet data into Delta Lake format. 

# COMMAND ----------

# Configure Delta Lake Silver Path
deltalake_silver_path <- "/ml/loan_by_state_delta"

# Remove folder if it exists
dbutils.fs.rm(deltalake_silver_path, recurse = 'True')

# COMMAND ----------

# Remove existing table
sql("DROP TABLE IF EXISTS loan_by_state_delta")

# Create table using Delta Lake 
sql(
    "CREATE TABLE loan_by_state_delta
     USING delta
     LOCATION '/ml/loan_by_state_delta'
     AS SELECT * FROM loan_by_state"
)

# Show results
display(tableToDF("loan_by_state_delta"))

# COMMAND ----------

# SparkR: read from Delta
spark_df <- read.df(deltalake_silver_path, source = "delta")
spark_df <- tableToDF("loan_by_state_delta")

# COMMAND ----------

# It also works perfectly fine with Sparklyr
sc <- sparklyr::spark_connect(method = "databricks")
sparklyr_table_df <- sparklyr::spark_read_table(sc, "loan_by_state_delta")
sparklyr_df <- sparklyr::spark_read_delta(sc, deltalake_silver_path)

# COMMAND ----------

display(sql("DESCRIBE DETAIL loan_by_state_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the notebook before the streaming cell, in case of a "run all" 

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit("stop") 

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `loan_stats_delta` table
# MAGIC * We will run two streaming queries concurrently against this data
# MAGIC * Note, you can also use `writeStream` but this version is easier to run in DBCE

# COMMAND ----------

# MAGIC %r
# MAGIC # Read the insertion of data
# MAGIC loan_by_state_readStream <- read.stream(source = "delta", path = deltalake_silver_path)
# MAGIC createOrReplaceTempView(loan_by_state_readStream, "loan_by_state_readStream")
# MAGIC 
# MAGIC # Kick off the stream
# MAGIC #sql("SELECT addr_state, sum(`count`) AS loans FROM loan_by_state_readStream GROUP BY addr_state")

# COMMAND ----------

# MAGIC %sql
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_readStream group by addr_state

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running before executing the code below

# COMMAND ----------

i <- 1
while (i <= 6){
  # Execute Insert statement
  insert_sql <- "INSERT INTO loan_by_state_delta VALUES ('IA', 450)"
  sql(insert_sql)
  print(paste0("loan_by_state_delta: inserted new row of data, loop: [", i, "]"))
  
  # Wait to insert the next batch
  i <- i + 1
  Sys.sleep(time = 3)
  }

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Once the previous cell is finished and the state of Iowa is fully populated in the map (in cell 14), click *Cancel* in Cell 14 to stop the `readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's review our current set of loans using our map visualization.

# COMMAND ----------

# Convert table to DF, count the number of loans in each state
tableToDF("loan_by_state_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md Observe that the Iowa (middle state) has the largest number of loans due to the recent stream of data.  Note that the original `loan_by_state_delta` table is updated as we're reading `loan_by_state_readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support
# MAGIC 
# MAGIC **Note**: Full DML Support is a feature that will be coming soon to Delta Lake; the preview is currently available in Databricks.
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

# Load new DataFrame based on current Delta table
lbs_df <- tableToDF("loan_by_state_delta")

# Save DataFrame to Parquet
# Note: For ease of demonstration this is being saved on the driver
write.parquet(lbs_df, path = "loan_by_state.parquet", mode = 'overwrite')

# Reload Parquet Data & create new table
lbs_pq <- read.parquet("loan_by_state.parquet")

# Create temp view
createOrReplaceTempView(lbs_pq, "loan_by_state_pq")

# Review data
display(lbs_pq)

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC 
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `DELETE` those values assigned to `IA`

# COMMAND ----------

sql("DELETE FROM loan_by_state_pq WHERE addr_state = 'IA'")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# Running `DELETE` on the Delta Lake table
sql("DELETE FROM loan_by_state_delta WHERE addr_state = 'IA'")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_by_state_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `UPDATE` those values

# COMMAND ----------

# Attempting to run `UPDATE` on the Parquet table
sql("UPDATE loan_by_state_pq SET n = 2700 WHERE addr_state = 'WA'")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `UPDATE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# Running `UPDATE` on the Delta Lake table
sql("UPDATE loan_by_state_delta SET `count` = 2700 WHERE addr_state = 'WA'")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_by_state_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Let's create a simple R dataframe to merge
merge_df <- data.frame(
  addr_state = c("IA", "CA", "OR"),
  count = c(0, 2500, 0)
)

# Copy to Spark and create temp view
mergeDF <- createDataFrame(merge_df) 
createOrReplaceTempView(mergeDF, "merge_table")

display(mergeDF)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

sql(
  "
MERGE INTO loan_by_state_delta as d
USING merge_table as m
on d.addr_state = m.addr_state
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *
")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_by_state_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# MAGIC %r
# MAGIC # Generate new loans with dollar amounts 
# MAGIC loans <- sql(
# MAGIC   "
# MAGIC SELECT addr_state, 
# MAGIC CAST(RAND(10)*`count` AS bigint) AS count,
# MAGIC CAST(RAND(10) * 10000 * `count` AS double) AS amount
# MAGIC FROM loan_by_state_delta
# MAGIC ")
# MAGIC 
# MAGIC display(loans)

# COMMAND ----------

# MAGIC %r
# MAGIC # Let's write this data out to our Delta table
# MAGIC write.df(loans, 
# MAGIC          source = "delta", 
# MAGIC          path = deltalake_silver_path, 
# MAGIC          mode = "append")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the schema of our new data does not match the schema of our original data

# COMMAND ----------

# Add the mergeSchema option
write.df(loans, 
         source = "delta", 
         path = deltalake_silver_path, 
         mode = "append",
         mergeSchema = TRUE)

# COMMAND ----------

# MAGIC %md **Note**: With the `mergeSchema` option, we can merge these different schemas together.

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
display(sql(
  "SELECT addr_state,
   SUM(`amount`) AS amount
   FROM loan_by_state_delta
   GROUP BY addr_state
   ORDER BY SUM(`amount`)
   DESC LIMIT 10"
))

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or R syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

display(sql("DESCRIBE HISTORY loan_by_state_delta"))

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

display(sql("SELECT * FROM loan_by_state_delta VERSION AS OF 0"))

# COMMAND ----------

display(sql("SELECT * FROM loan_by_state_delta VERSION AS OF 9"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC -----
# MAGIC #### Appendix

# COMMAND ----------

# MAGIC %fs ls /ml/loan_by_state_delta/_delta_log

# COMMAND ----------

# MAGIC %fs head ml/loan_by_state_delta/_delta_log/00000000000000000010.json