# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks for the R Data Scientist
# MAGIC 
# MAGIC <img src='https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png' width="225" />
# MAGIC <img src='https://www.rstudio.com/wp-content/uploads/2014/06/RStudio-Ball.png' width="120" />
# MAGIC 
# MAGIC This notebook is designed to help orient the R user to Databricks.  It covers some of the features unique to Databricks Notebooks as well as a basic introduction to the two packages for working with Spark in R.  
# MAGIC 
# MAGIC **Contents**
# MAGIC 
# MAGIC I. Building Powerful Notebooks
# MAGIC 
# MAGIC * Databricks File System -- DBFS
# MAGIC * Magic Commands -- %python, %sql, and more
# MAGIC * `dbutils` -- Handy functions to make you more productive
# MAGIC * Widgets -- Paramaterize your notebook
# MAGIC 
# MAGIC II. Databricks Runtime with Apache Spark
# MAGIC 
# MAGIC * SparkR & sparklyr
# MAGIC * Visualization with `display()`
# MAGIC 
# MAGIC * * *
# MAGIC 
# MAGIC ## I. Building Powerful Notebooks
# MAGIC 
# MAGIC **DBFS**
# MAGIC 
# MAGIC If you are wondering whether you have a file system to work with, the answer is yes!
# MAGIC 
# MAGIC Databricks File System (DBFS) is a distributed file system installed on Databricks clusters. Files in DBFS persist to object storage, so you won’t lose data even after you terminate a cluster.  Even better, you get the benefits of flexible cloud storage and the organizing structure of a file system.
# MAGIC 
# MAGIC Let's take a look at what we find in the `/databricks-datasets` directory on DBFS.

# COMMAND ----------

# DBFS is mounted to the local file system on the cluster
system("ls /dbfs/databricks-datasets", intern = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can read more about DBFS in the [R User Guide](https://github.com/marygracemoesta/R-User-Guide/blob/master/Getting_Started/DBFS.md) and in the [official Databricks docs](https://docs.databricks.com/data/databricks-file-system.html#databricks-file-system-dbfs).
# MAGIC 
# MAGIC **Magic Commands**
# MAGIC 
# MAGIC You can override the primary language of a notebook by specifying the language magic command %<language> at the beginning of a cell. The supported magic commands are: `%python`,`%r`, `%scala`, and `%sql`. In addition, you can run shell commands using `%sh`,  document your work with Markdown syntax by using `%md`, and execute other notebooks using `%run`.  
# MAGIC   
# MAGIC These magic commands are a powerful way to open up your notebook to specialized libraries in other languages as well as assets you may have created in other notebooks.  For example, save some `ggplot2` or `plotly` output to DBFS and render them in a new notebook using Markdown.  To dive deeper please see the [official documentation for Databricks Notebooks](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html).
# MAGIC   
# MAGIC * * * 
# MAGIC   
# MAGIC **Databricks Utilities**
# MAGIC 
# MAGIC Databricks Utilities (`dbutils`) are a set of functions that designed to empower you and your productivity inside of Databricks.
# MAGIC 
# MAGIC With the exception of [file system utilities](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-fs), all `dbutils` functions are available for R notebooks. For example, [secrets utilities](https://docs.azuredatabricks.net/user-guide/dev-tools/dbutils.html#secrets-utilities) let you work with authentication variables (like JDBC credentials) without having to display sensitive information in your notebook:

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Widgets**
# MAGIC 
# MAGIC One particularly useful set of `dbutils` functions are [Widgets](https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#widgets).  Widgets add a layer of parameterization to your notebooks that can be used when building dashboards and visualizations, training models, or querying data sources.  Think of widgets as turning your notebook into a function.
# MAGIC 
# MAGIC In the following example we'll create a widget to dynamically render the output of a plot.

# COMMAND ----------

## Define the choices for our dropdown widget
cols <- as.list(colnames(mtcars))

## Instatiate the widgets
dbutils.widgets.dropdown(name = "X-Variable", defaultValue = "mpg", choices = cols)
dbutils.widgets.dropdown(name = "Y-Variable", defaultValue = "disp", choices = cols)

## Store our widget value
xValue <- dbutils.widgets.get("X-Variable")
yValue <- dbutils.widgets.get("Y-Variable")

## Quick plot of mtcars using dynamic widget values 
plot(x = mtcars[, xValue], xlab = xValue,
     y = mtcars[, yValue], ylab = yValue)

## Works with ggplot as well
# library(ggplot2)
# ggplot2::ggplot(mtcars, ggplot2::aes(mtcars[, xValue], mtcars[, yValue])) + geom_point()

# COMMAND ----------

# MAGIC %md
# MAGIC Changing the values in each widget will automatically rerun the cell and render fresh output.  Give it a shot!
# MAGIC 
# MAGIC ***
# MAGIC 
# MAGIC Now that you've learned how to parameterize your notebooks with widgets, let's remove them before moving to the second topic in our notebook.

# COMMAND ----------

## Remove all widgets from this notebook
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## II. Databricks Runtime with Apache Spark
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/11/Runtime-OG.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Runtime improves the user experience of developing in Apache Spark by offering better performance, reliability, and security.  This is achieved by a series of optimizations and service features, such as: <br><br>
# MAGIC 
# MAGIC * **Databricks I/O** - Higher S3/Blob storage throughput, data skipping, transparent caching
# MAGIC * **Serverless Infrastructure** - Shared pools, auto-configuration, auto-scaling, reliable fine-grained sharing
# MAGIC 
# MAGIC Practically speaking, what this means is that you can port your existing Spark code to Databricks Runtime and [expect it to perform better](https://databricks.com/blog/2017/10/06/accelerating-r-workflows-on-databricks.html).  Now, if you are an R developer writing Spark jobs, chances are you are going to be writing that code in one of two APIs for Spark.  Let's cover those now.
# MAGIC 
# MAGIC **Note:  In order to see Spark jobs running with `sparklyr`, the entire library must be loaded.  This notebook only loads `SparkR` and references specific functions from `sparklyr` and `dplyr` when needed using `library::function()` syntax.  **
# MAGIC 
# MAGIC * * *
# MAGIC 
# MAGIC **SparkR & sparklyr**
# MAGIC 
# MAGIC Both libraries are highly capable of working with big data in R - as of Feb. '19 their feature sets are essentially at parity.  Perhaps the biggest difference between the two is `sparklyr` was designed using the `dplyr` approach to data analysis.  The consequences of this are that `sparklyr` functions return a _reference_ to a Spark DataFrame which can be used as a `dplyr` table ([source](https://spark.rstudio.com/dplyr/)).
# MAGIC 
# MAGIC Let's take a look at this phenomenon by loading data from the _airlines_ dataset with each API.  We'll use `SparkR` for the year 2008 and `sparklyr` for 2007.

# COMMAND ----------

## Load SparkR & magrittr
suppressMessages(library(SparkR))
suppressMessages(library(magrittr))

## sparklyr requires creating a connection to Spark first
sc <- sparklyr::spark_connect(method = "databricks")

## Read airlines dataset from 2008
airlinesDF <- read.df("/databricks-datasets/asa/airlines/2008.csv", source = "csv", inferSchema = "true", header = "true")

# If file was in parquet format
# airlinesDF <- read.parquet("/databricks-datasets/asa/airlines/2008/")

## In sparklyr we specify the spark connection, read data from 2007
sparklyAirlines <- sparklyr::spark_read_csv(sc, name = 'airlines', path = "/databricks-datasets/asa/airlines/2007.csv")

## Check the class of each loaded dataset
cat(c("Class of SparkR object:\n", class(airlinesDF), "\n\n"))
cat(c("Class of sparklyr object:\n", class(sparklyAirlines)))

# COMMAND ----------

# MAGIC %md
# MAGIC As you might expect, calling `SparkR` functions on `sparklyr` objects and vice versa can lead to unexpected -- and undesirable -- behavior.  Why is this?
# MAGIC 
# MAGIC `dplyr` functions that work with Spark are translated into SparkSQL statements, and always return a SparkSQL table.  This is **not** the case with the `SparkR` API, which has functions for SparkSQL tables and Spark DataFrames.  As such, the interoperability between the two APIs is limited and it is generally not recommended to go back and forth between them in the same job. 
# MAGIC 
# MAGIC Let's look at what happens when we run a `sparklyr` command on a Spark DataFrame and a `SparkR` command on a `dplyr` Spark table.

# COMMAND ----------

## Function from sparklyr on SparkR object
sparklyr::sdf_pivot(airlinesDF, DepDelay ~ UniqueCarrier)

# COMMAND ----------

## Function from SparkR on sparklyr object
SparkR::arrange(sparklyAirlines, "DepDelay")

# COMMAND ----------

# MAGIC %md
# MAGIC Alright, so that is kind of confusing and might give you pause.  
# MAGIC 
# MAGIC You might be asking yourself, "Is there any space in the APIs where they can work together?"  
# MAGIC 
# MAGIC Well, in my research so far I have found one intersection between the two APIs, and that is SparkSQL.  Recall that when we loaded the airlines data from 2007 into a `tbl_spark`, we specified the table name _airlines_.  This table is registered with SparkSQL and can be referenced using `SparkR`'s SparkSQL functions.  These SQL queries will return a Spark DataFrame.

# COMMAND ----------

## Use SparkR to query a table loaded into SparkSQL through sparklyr
top10delaysDF <- SparkR::sql("SELECT UniqueCarrier, DepDelay, Origin FROM airlines WHERE DepDelay NOT LIKE 'NA' ORDER BY DepDelay DESC LIMIT 10")

## Check class of result
cat(c("Class of top10delaysDF: ", class(top10delaysDF), "\n\n"))

## Inspect the results
cat("Top 10 Airline Delays for 2007:\n")
head(top10delaysDF, 10)

# COMMAND ----------

# MAGIC %md
# MAGIC Still, I don't recommend taking this approach.  Make your life simple and pick one API to build your job around!
# MAGIC 
# MAGIC * * *
# MAGIC 
# MAGIC **Visualize with `display()`**
# MAGIC 
# MAGIC One of the challenges in big data is visualizing large datasets.  Single node R has memory limitations, and plotting all 700 million rows in your data will likely wind up looking like a mess.  There are a few ways to deal with this problem, but perhaps the simplest of them is the `display()` function that is unique to Databricks.  Passing a Spark DataFrame to `display()` will render a sortable table that can also be configured as a visualization.
# MAGIC 
# MAGIC Follow the instructions in the cell below to build a bar chart from a Spark DataFrame of over 7 million rows.

# COMMAND ----------

## Display the airlinesDF
display(airlinesDF)

## To configure a bar chart click on the bar icon and select "Bar", then click on "Plot Options"
##
## 1. Drag columns from 'All Fields' to the right to build your plot.  Start by dragging 'UniqueCarrier' to Keys.
## 2. Next, drag 'DayOfWeek' to Series groupings and 'DepDelay' to Values.
## 3. Make sure the aggregation is set to COUNT.
## 4. Click on Apply to start the Spark job that will run the groupBy aggregations and build the plot. 
##
## You should see WN (Southwest) and AA (American Airlines) having the greatest sum of flight distance.

# COMMAND ----------

# MAGIC %md
# MAGIC The rule of thumb for visualizing data in Spark DataFrames is to first aggregate or [sample](https://spark.apache.org/docs/latest/api/R/sample.html) the data, collect it to the driver in a local R data.frame, then use your favorite library for plotting.  
# MAGIC 
# MAGIC For example, if we wanted to create the same plot as we got before from `ggplot2`, we would proceed as follows:

# COMMAND ----------

## Group the data by carrier and day of week, get the sum of Distance for each group
distanceByCarrierDF <- summarize(groupBy(airlinesDF, "UniqueCarrier", "DayOfWeek"), DistanceTotal = sum(airlinesDF$Distance))

## Collect the results back locally
dist_agg_df <- collect(distanceByCarrierDF)

## Plot
options(repr.plot.width=3600, repr.plot.height=1200)

p <- ggplot2::ggplot(dist_agg_df, ggplot2::aes(x = UniqueCarrier, y = DistanceTotal/1000000, fill = as.factor(DayOfWeek))) + 
        ggplot2::geom_bar(stat = "identity", position = "dodge") +
        ggplot2::geom_bar(stat = "identity", position = 'dodge', color = 'white') +
        ggplot2::scale_y_continuous(labels = scales::unit_format(unit = "M")) + 
        ggplot2::scale_fill_discrete(name = "Day of Week") +
        ggplot2::labs(y = 'Distance') +
        ggplot2::theme_minimal() 

p

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC This concludes the initial introduction to Databricks Notebooks and the available packages for Spark in R!