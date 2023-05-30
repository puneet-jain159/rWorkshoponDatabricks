# Databricks notebook source
# MAGIC %md
# MAGIC ## Distributed R: User Defined Functions in Spark
# MAGIC 
# MAGIC **Contents**
# MAGIC 
# MAGIC * Understanding UDFs
# MAGIC * Distributed `apply`
# MAGIC   * `spark_apply`
# MAGIC   * `dapply` & `gapply`
# MAGIC   * `spark.lapply`
# MAGIC * Leveraging R Packages Inside a UDF
# MAGIC * Apache Arrow
# MAGIC * Additional Examples
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC #### Understanding UDFs
# MAGIC 
# MAGIC Both `SparkR` and `sparklyr` support user-defined functions (UDFs) in R which allow you to execute arbitrary R code across a cluster.  The advantage here is the ability to distribute the computation of functions included in R's massive ecosystem of 3rd party packages.  In particular, you may want to use a domain-specific package for machine learning or apply a specific statisical transformation that is not available through the Spark API.  Running in-house custom R libraries on larger data sets would be another place to use this family of functions.
# MAGIC 
# MAGIC How do these functions work?  The R process on the driver has to communicate with R processes on the worker nodes through a series of serialize/deserialize operations through the JVMs.  The following illustration walks through the steps required to run arbitrary R code across a cluster.
# MAGIC 
# MAGIC <img src="https://github.com/kurlare/misc-assets/blob/master/DistributedR_ControlFlow.png?raw=true" width="800" />
# MAGIC 
# MAGIC Looks great, but what's the catch?  
# MAGIC 
# MAGIC * **You have to reason about your program carefully and understand how exactly these functions are being, *ahem*, applied across your cluster.**  
# MAGIC 
# MAGIC * **R processes on worker nodes are ephemeral.  When the function being applied finishes execution the process is shut down and all state is lost.**
# MAGIC 
# MAGIC * **As a result, you have to pass any contextual data and libraries along with your function to each worker for your job to be successful.**
# MAGIC 
# MAGIC * **There is overhead related to creating the R process and ser/de operations in each worker.**  
# MAGIC 
# MAGIC Don't be surprised if using these functions runs slower than expected, especially on the first pass if you have to install the packages in each worker.  One of the benefits of running distributed R on Databricks is that you can install libraries at the cluster scope.  This makes them available on each worker and you do not have to pay this performance penalty every time you spin up a cluster.
# MAGIC 
# MAGIC The general best practice is to leverage the Spark API first and foremost, then if there is no way to implement your logic except in R you can turn to UDFs and get the job done.  This is echoed [here](https://therinspark.com/distributed.html) by one of the authors of `sparklyr` who is currently at RStudio.
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC #### Distributed `apply`
# MAGIC 
# MAGIC Between `sparklyr` and `SparkR` there are a number of options for how you can distribute your R code across a cluster with Spark.  Functions can be applied to each *group* or each *partition* of a Spark DataFrame, or to a list of elements in R.  In the following table you can see the whole family of distributed `apply` functions:
# MAGIC 
# MAGIC |  Package | Function Name |     Applied To     |   Input  |    Output    |
# MAGIC |:--------:|:-------------:|:------------------:|:--------:|:-----------:|
# MAGIC | sparklyr |  spark_apply  | partition or group | Spark DF |   Spark DF  |
# MAGIC |  SparkR  |     dapply    |      partition     | Spark DF |   Spark DF  |
# MAGIC |  SparkR  | dapplyCollect |      partition     | Spark DF | R dataframe |
# MAGIC |  SparkR  |     gapply    |        group       | Spark DF |   Spark DF  |
# MAGIC |  SparkR  | gapplyCollect |        group       | Spark DF | R dataframe |
# MAGIC |  SparkR  |  spark.lapply |    list element    |   list   |     list    |
# MAGIC 
# MAGIC Let's work through these different functions one by one.
# MAGIC 
# MAGIC ##### `gapply` (and `dapply`)
# MAGIC 
# MAGIC For the first example, we'll use **`gapply()`**.
# MAGIC 
# MAGIC `gapply` takes a Spark DataFrame as input and must return a Spark DataFrame as well.  It will execute the function against each group of the which we will specify with a 'group by' column in the function call. 
# MAGIC 
# MAGIC In `SparkR` there is also `dapply`, which lets you run your R code on each partition of the data, rather than each group. `gapply` tends to be used much more often however.
# MAGIC 
# MAGIC Let's test `gapply` by loading some airlines data into Spark and creating a new column for each Unique Carrier inside the R process on the workers:
# MAGIC 
# MAGIC **Note:** To get the best performance, we specify the schema of the expected output DataFrame to `gapply`.  This is optional, but if we don't supply the schema Spark will need to sample the output to infer it.  This can be quite costly on longer running UDFs.

# COMMAND ----------

library(magrittr)
library(SparkR)

# COMMAND ----------

# Define shema's
sparkR_schema <- structType(
  structField("year", "integer"),
  structField("Month", "integer"),
  structField("DayofMonth","integer"),
  structField("DayOfWeek","integer"),
  structField("DepTime","integer"),
  structField("CRSDepTime","integer"),
  structField("ArrTime", "integer"),
  structField("CRSArrTime", "integer"),
  structField("UniqueCarrier","string"),
  structField("FlightNum","integer"),
  structField("TailNum","string"),
  structField("ActualElapsedTime","integer"),
  structField("CRSElapsedTime","integer"),
  structField("AirTime","integer"),
  structField("ArrDelay","integer"),
  structField("DepDelay","integer"),
  structField("Origin","string"),
  structField("Dest","string"),
  structField("Distance","integer"),
  structField("TaxiIn","integer"),
  structField("TaxiOut","integer"),
  structField("Cancelled","integer"),
  structField("CancellationCode","string"),
  structField("Diverted","integer"),
  structField("CarrierDelay","string"),
  structField("WeatherDelay","string"),
  structField("NASDelay","string")
)

# COMMAND ----------

airlines_path <- "databricks-datasets/asa/airlines/2007.csv"
airlinesDF <- SparkR::read.df(path = paste0("dbfs:/", airlines_path), source = "csv", header = "true", inferSchema = "false", schema = sparkR_schema, na.strings = "NA")

# Define output schema
schema <- structType(structField("UniqueCarrier", "string"),
                     structField("newcol", "string"))

resultsDF <- gapply(airlinesDF,
                   cols = "UniqueCarrier",
                   function(key, e){
                     one_carrier_df <- data.frame(
                       UniqueCarrier = unique(e$UniqueCarrier), 
                       newcol = paste0(unique(e$UniqueCarrier), "_new")
                     )
                     one_carrier_df
                   },
                   schema = schema)

head(resultsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC 
# MAGIC ### Leveraging Packages in Distributed R
# MAGIC 
# MAGIC As stated above, everything required for your function in `gapply` needs to be passed along with it.  On Databricks you can install packages on the cluster and they will automatically be installed on each worker.  This saves you time and gives you two options to use libraries in `gapply` on Databricks:
# MAGIC <br><br>
# MAGIC 
# MAGIC * Load the entire library - `library(broom)`
# MAGIC * Reference a specific function from the library namespace - `broom::tidy()`
# MAGIC 
# MAGIC In a less trivial example of `gapply`, we can train a model on each partition. We begin by grouping the input data by `Origin`, then specify our function to apply.  This will be a simple model where our dependent variable is Arrival Delay (`ArrDelay`) and our independent variable is Departure Delay (`DepDelay`) from the month of December.  Furthermore, we can use the `broom` package to tidy up the output of our linear model.  The results will be a Spark DataFrame with different coefficients for each group.
# MAGIC 
# MAGIC **Note:** Make sure you attach the `broom` package to your cluster before you run the next cell.

# COMMAND ----------

featuresDF <- airlinesDF %>% 
filter(column("Month") == 12) %>% 
select("Origin", "DepDelay", "ArrDelay") %>% na.omit()

## Define output schema
result_schema <- structType(
  structField("term","string"),
  structField("estimate","double"),
  structField("std_error","double"),
  structField("statistic","double"),
  structField("p_value","double"),
  structField("origin","string")
)

resultsDF <- gapply(featuresDF,
                   cols = "Origin",
                   function(key, e){
                     print(key)
                     e$ArrDelay <- as.numeric(e$ArrDelay)
                     e$DepDelay <- as.numeric(e$DepDelay)
                     output_df <- broom::tidy(lm(ArrDelay ~ DepDelay, data = e))
                     output_df$origin <- key[[1]]
                     output_df
                   },
                   schema = result_schema)

head(resultsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDFs with Sparklyr
# MAGIC 
# MAGIC In the below example, we'll use **`spark_apply()`**, which is essentially the `sparklyr` version of `gapply` and `dapply`
# MAGIC 
# MAGIC `spark_apply` takes a Spark DataFrame as input and must return a Spark DataFrame as well.  By default it will execute the function against each partition of the data, but we will change this by specifying a 'group by' column in the function call.  `spark_apply()` will also distribute all of the contents of your local `.libPaths()` to each worker when you call it for the first time unless you set the `packages` parameter to `FALSE`.  For more details see the [Official Documentation](https://spark.rstudio.com/guides/distributed-r/).  Let's test this by loading some airlines data into Spark and creating a new column for each Unique Carrier inside the R process on the workers:
# MAGIC 
# MAGIC **Note:** To get the best performance, we specify the schema of the expected output DataFrame to `spark_apply`.  This is optional, but if we don't supply the schema Spark will need to sample the output to infer it.  This can be quite costly on longer running UDFs.

# COMMAND ----------

# Read data into Spark
sc <- sparklyr::spark_connect(method = "databricks")

sparklyAirlines <- sparklyr::spark_read_csv(sc, name = 'airlines', path = "/databricks-datasets/asa/airlines/2007.csv")

## Take a subset of the columns
subsetDF <- dplyr::select(sparklyAirlines, UniqueCarrier, Month, DayofMonth, Origin, Dest, DepDelay, ArrDelay) 

## Focus on the month of December, Christmas Eve
holidayTravelDF <- dplyr::filter(subsetDF, Month == 12, DayOfMonth == 24)

## Add a new column for each group and return the results
resultsDF <- sparklyr::spark_apply(holidayTravelDF,
                                  group_by = "UniqueCarrier",
                                  function(e){
                                    # 'e' is a data.frame containing all the rows for each distinct UniqueCarrier
                                    one_carrier_df <- data.frame(newcol = paste0(unique(e$UniqueCarrier), "_new"))
                                    one_carrier_df
                                  }, 
                                   # Specify schema
                                   columns = list(
                                   UniqueCarrier = "string",
                                   newcol = "string"),
                                   # Do not copy packages to each worker
                                   packages = F)
head(resultsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Arrow
# MAGIC 
# MAGIC [Apache Arrow](https://arrow.apache.org/) is a project that aims to improve analytics processing performance by representing data in-memory in columnar format and taking advantage of modern hardware.  The main purpose and benefit of the project can be summed up in the following image, taken from the homepage of the project.
# MAGIC 
# MAGIC <img src="https://github.com/kurlare/misc-assets/blob/master/apache-arrow.png?raw=true">
# MAGIC 
# MAGIC Arrow is highly effective at speeding up data transfers.  It's worth mentioning that [Databricks Runtime offers a similar optimization](https://databricks.com/blog/2018/08/15/100x-faster-bridge-between-spark-and-r-with-user-defined-functions-on-databricks.html) out of the box with SparkR.  This is not currently available for `sparklyr`, so if you want to use that API it's recommended to install Arrow.
# MAGIC 
# MAGIC We'll run a benchmark test with `spark_apply` and compare performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Installing Arrow for R for Databricks Runtime 7.X and higher (for Databricks Runtime <7 see Appendix below)
# MAGIC * Install arrow from CRAN in the Cluster UI.
# MAGIC * Add `spark.sql.execution.arrow.sparkr.enabled` true to the 'Spark' tab under 'Advanced Settings' in the Cluster UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Benchmark on Arrow vs non-Arrow

# COMMAND ----------

library(bench)
library(arrow) 

bench::press(rows = c(10^5, 10^6), {
  bench::mark(
    arrow_on = {
      suppressMessages(library(arrow))
      sparklyr::sdf_len(sc, rows) %>% sparklyr::spark_apply(~ .x / 2) %>% dplyr::count() %>% sparklyr::collect()
    },
    arrow_off = if (rows <= 10^5) {
      if ("arrow" %in% .packages()) detach("package:arrow")
      sparklyr::sdf_len(sc, rows) %>% sparklyr::spark_apply(~ .x / 2) %>% dplyr::count() %>% sparklyr::collect()
    } else NULL, iterations = 4, check = FALSE)
})

# COMMAND ----------

# MAGIC %md
# MAGIC You can see the massive speedup from the common in memory data format.  Ser/de overhead begone!
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ### Additional UDF Examples
# MAGIC 
# MAGIC **`dapply()`**
# MAGIC 
# MAGIC In the following example we will isolate the `DepDelay` column in our airlines dataset and apply a jitter to each of the numeric values.  While this is a simple operation, there is no native support for `jitter()` in SparkR. 

# COMMAND ----------

## Select one column to reduce data volume being sent to each worker
depDelayDF <- select(airlinesDF, "DepDelay")

## Remove null values from the column
depDelayDF <- filter(depDelayDF, isNotNull(depDelayDF$DepDelay))

## With dapply() we need to provide a schema for the returning DataFrame
## Passing the wrong type will yield incorrect results (i.e, integer instead of double)
resultSchema <- structType(
  structField("DepDelay", "integer"),
  structField("jitteredDelay", "double")
)

## Apply a function to jitter the value of each DepDelay and return 
## the original and transformed columns
jitteredDF <- dapply(depDelayDF,
                     function(x) {
                       library(arrow)
                       x <- cbind(x, jitter(x$DepDelay))},
                     resultSchema)

## Display results & class
cat("Result of dapply: \n")
print(head(jitteredDF))
cat(c("\nClass of result from dapply():", class(jitteredDF)))

# COMMAND ----------

# MAGIC %md
# MAGIC **`dapplyCollect()`**
# MAGIC 
# MAGIC This function takes a Spark DataFrame as input and returns a regular R data.frame.  As such, it does not require a schema to be passed with it.  The catch here is that the collective results of your UDF need to be able to fit into the driver.  
# MAGIC 
# MAGIC Let's execute the same 'jitter' operation as before, but this time we'll index each partition and only return the first row.  This is useful because row indexing in Spark is not typically possible.  For this task we can reuse the `DepDelayDF` created above.

# COMMAND ----------

## Apply a function to jitter the value of each DepDelay and return 
## the first row of the original and transformed columns
jittered_df <- dapplyCollect(depDelayDF, function(x) {x <- data.frame(DepDelay = x[1,], 
                                                                      jittered = jitter(x$DepDelay[1]))})

## Display results & class
cat("Result of dapplyCollect: \n")
print(head(jittered_df))
cat(c("\nClass of result from dapplyCollect():", class(jittered_df)))
cat(c("\n\nNumber of records in jittered_df:", nrow(jittered_df)))
cat(c("\n\nNumber of partitions in depDelayDF:", getNumPartitions(depDelayDF)))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that we have one result for each partition, because the function was applied to each partition.
# MAGIC 
# MAGIC ##### `spark.lapply` : Using a list as input
# MAGIC 
# MAGIC This function is also from SparkR.  It accepts a list and then uses Spark to apply R code to each element in the list across the cluster.  As [the docs](https://spark.apache.org/docs/latest/api/R/spark.lapply.html) state, it is conceptually similar to `lapply` in base R, so it will return a **list** back to the driver.  
# MAGIC 
# MAGIC For this example we'll take a list of strings and manipulate them in parallel, somewhat similar to the examples we've seen so far.

# COMMAND ----------

# Create list of strings
carriers <- list("UA", "AA", "NW", "EV", "B6", "DL",
                 "OO", "F9", "YV", "AQ", "US", "MQ",
                 "OH", "HA", "XE", "AS", "CO", "FL",
                 "WN", "9E")

list_of_dfs <- spark.lapply(carriers, 
                            function(e) {
                              data.frame(UniqueCarrier = e,
                                         newcol = paste0(e, "_new"))
                            })

# Convert the list of small data.frames into a tidy single data.frame
tidied <- data.frame(t(sapply(list_of_dfs, unlist)))

display(tidied)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC 
# MAGIC This concludes the lesson on UDFs with Spark in R.  If you want to learn more, here are additional resources about distributed R with Spark.
# MAGIC 
# MAGIC 1. [100x Faster Bridge Between R and Spark on Databricks](https://databricks.com/blog/2018/08/15/100x-faster-bridge-between-spark-and-r-with-user-defined-functions-on-databricks.html)
# MAGIC 2. [Shell Oil: Parallelizing Large Simulations using SparkR on Databricks](https://databricks.com/blog/2017/06/23/parallelizing-large-simulations-apache-sparkr-databricks.html)
# MAGIC 3. [Distributed R Chapter from 'The R in Spark'](https://therinspark.com/distributed.html)
# MAGIC 
# MAGIC *** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Appendix: Installing Arrow for R for Databricks Runtime 6.X and lower
# MAGIC Run the following cell to save the init script to a location of your choice on DBFS. 

# COMMAND ----------

# MAGIC %python
# MAGIC ## Define contents of the script
# MAGIC script = """
# MAGIC #!/bin/bash
# MAGIC git clone https://github.com/apache/arrow
# MAGIC cd arrow/r
# MAGIC R CMD INSTALL .
# MAGIC Rscript -e "arrow::install_arrow()"
# MAGIC """
# MAGIC 
# MAGIC ## Create directory to save the script in
# MAGIC dbutils.fs.mkdirs("/databricks/arrow")
# MAGIC 
# MAGIC ## Save the script to DBFS
# MAGIC dbutils.fs.put("/databricks/arrow/arrow-install.sh", script, True)

# COMMAND ----------

# MAGIC %md 
# MAGIC Next, open the Cluster UI edit the existing configuration for your cluster.  Head to _Advanced Options_ and update the _Init Scripts_ section to include the path to your script defined above.  It should look something like this:
# MAGIC <img src="https://github.com/kurlare/misc-assets/blob/master/Screen%20Shot%202019-03-19%20at%2011.29.15%20AM.png?raw=true" width = 800 height = 800>
# MAGIC <p>
# MAGIC Restart the cluster (this might take 10-15 minutes) and then run the benchmark in the following cell.