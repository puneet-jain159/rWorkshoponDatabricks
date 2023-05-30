# Databricks notebook source
# MAGIC %md
# MAGIC ## Replicating Single Node R Workloads on Databricks
# MAGIC 
# MAGIC Unless Spark is invoked, all work in an R notebook (or cell) is **only executed on the driver**.  Databricks Runtime includes a number of [pre-installed R packages](https://docs.databricks.com/release-notes/runtime/6.5.html#installed-r-libraries), but other than that you are essentially given a fresh R environment to do your work.
# MAGIC 
# MAGIC What follows are some simple examples of R code taken from the [dplyr vignettes](https://cran.r-project.org/web/packages/dplyr/vignettes/dplyr.html).

# COMMAND ----------

# Install and load packages
install.packages("nycflights13")
library(dplyr)
library(ggplot2)
library(nycflights13)

# COMMAND ----------

# Check the dimensions of the data
dim(flights)

# COMMAND ----------

# Inspect the first few rows
head(flights)

# COMMAND ----------

# Perform some dplyr commands 
flights %>% 
  group_by(month) %>% 
  summarise(count = n())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data to DBFS
# MAGIC When you want to save your data, you ideally want to save it to *persistent* storage. Remember, Databricks clusters are meant to be *ephemeral*, and as such are not meant for persistent storage. This means you will have to think differently about how to save your data and files when compared to working on your PC. 
# MAGIC 
# MAGIC To make this process easier, DBFS makes use of a FUSE mount. In short, what this means is that `dbfs:/` on the cloud is equivalent to `/dbfs/` on your driver (and worker storage!)
# MAGIC 
# MAGIC Lets try this with an example. we are going to:
# MAGIC 
# MAGIC * Create a folder on dbfs, called `dbfs:/rworkshop/flightsCSV/`
# MAGIC * Save our csv locally to `/dbfs/rworkshop/flightsCSV`
# MAGIC * Check that our csv exists in `dbfs:/`
# MAGIC 
# MAGIC Other notes on DBFS vs local (driver) file system:
# MAGIC 
# MAGIC * working with dbfs: use dbfs.fs or %fs commands
# MAGIC * working with local filesystem: use %sh (magic for executing shell commands)

# COMMAND ----------

## Create a new folder on DBFS
dbutils.fs.mkdirs("dbfs:/rworkshop/flightsCSV/")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/rworkshop/flightsCSV

# COMMAND ----------

# MAGIC %md
# MAGIC We can now write our R dataframe to the local filesystem as a CSV. The data will be persisted in cloud storage because of the FUSE mount on `dbfs:/`.

# COMMAND ----------

## Write to storage
flights %>% 
  write.csv("/dbfs/rworkshop/flightsCSV/flights.csv")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/rworkshop/flightsCSV/

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's clean up the files and folders we've created.  This can be done with `dbutils.fs` or in the shell with `%sh`.

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/rworkshop

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/rworkshop

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC 
# MAGIC ### Additional Examples
# MAGIC 
# MAGIC ##### Unix Operations
# MAGIC ```R
# MAGIC ## List files on DBFS
# MAGIC system("ls /dbfs/", intern = T)
# MAGIC 
# MAGIC ## Copy from one directory to another
# MAGIC system("cp /dbfs/directory_A /dbfs/directory_B")
# MAGIC 
# MAGIC ## Persist from driver to DBFS
# MAGIC system("cp /databricks/driver/file.txt /dbfs/file.txt")
# MAGIC ```
# MAGIC 
# MAGIC ##### Reading Data
# MAGIC 
# MAGIC **Note:** When reading into Spark, use the `dbfs:/` syntax.
# MAGIC 
# MAGIC ```R
# MAGIC ## Read a csv into local R dataframe
# MAGIC df <- read.csv("/dbfs/your/directory/file.csv")
# MAGIC 
# MAGIC ## Reading in your data from DBFS using SparkR
# MAGIC df <- read.df('dbfs:/your/directory/file.csv', 'csv')
# MAGIC 
# MAGIC ## Reading in your data from DBFS using sparklyr
# MAGIC df <- spark_read_csv(sc, 'df', 'dbfs:/your/directory/file.csv')
# MAGIC ```
# MAGIC 
# MAGIC ##### Saving & Loading Objects
# MAGIC 
# MAGIC ```R
# MAGIC ## Save a linear model to DBFS
# MAGIC model <- lm(mpg ~ ., data = mtcars)
# MAGIC saveRDS(model, file = "/dbfs/your/directory/model.RDS")
# MAGIC 
# MAGIC ## Load back into memory
# MAGIC model <- readRDS(file = "/dbfs/your/directory/model.RDS")
# MAGIC ```
# MAGIC 
# MAGIC ___
# MAGIC [To R User Guide](https://github.com/marygracemoesta/R-User-Guide#contents)