# Databricks notebook source
# MAGIC %md
# MAGIC ## Rstudio Integration
# MAGIC 
# MAGIC RStudio offers a polished experience for developing R programs that is impossible to reproduce in a notebook.  Databricks offers two choices for sticking with your favorite IDE - hosted in the cloud or on your local machine.  In this section we'll go over setup and best practices for both options.
# MAGIC 
# MAGIC **Contents**
# MAGIC 
# MAGIC * Hosted RStudio Server
# MAGIC   * Accessing Spark
# MAGIC   * Persisting Work
# MAGIC   * Git Integration
# MAGIC * RStudio Desktop with Databricks Connect
# MAGIC   * Setup
# MAGIC   * Limitations
# MAGIC  
# MAGIC  ___
# MAGIC  
# MAGIC ### Hosted RStudio Server
# MAGIC The simplest way to use RStudio with Databricks is to use Databricks Runtime (DBR) for Machine Learning 7.0+.  The open source version of Rstudio Server will automatically installed on the driver node of a cluster:
# MAGIC 
# MAGIC <img src="https://spark.rstudio.com/images/deployment/databricks/rstudio-databricks-local.png">
# MAGIC   
# MAGIC ___
# MAGIC 
# MAGIC You can then launch hosted RStudio Server from the 'Apps' tab in the Cluster UI:
# MAGIC 
# MAGIC <img src ="https://docs.databricks.com/_images/rstudio-apps-ui.png" height = 275 width = 1000>
# MAGIC 
# MAGIC For earlier versions of DBR you can attach the [RStudio Server installation script](https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/Customizing.md#rstudio-server-installation) to your cluster.  Full details for setting up hosted Rstudio Server can be found in the official docs [here](https://docs.databricks.com/spark/latest/sparkr/rstudio.html#get-started-with-rstudio-server-open-source).  If you have a license for RStudio Server Pro you can [set that up](https://docs.databricks.com/spark/latest/sparkr/rstudio.html#install-rstudio-server-pro) as well.
# MAGIC 
# MAGIC After the init script has been installed on the cluster, login information can be found in the `Apps` tab of the cluster UI as before.
# MAGIC 
# MAGIC The hosted RStudio experience should feel nearly identical to your desktop experience.  In fact, you can also customize the environment further by supplying an additional init script to [modify `Rprofile.site`](https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/Customizing.md#modifying-rprofile-in-rstudio).  
# MAGIC 
# MAGIC #### Accessing Spark
# MAGIC 
# MAGIC _(This section is taken from [Databricks documentation](https://docs.databricks.com/spark/latest/sparkr/rstudio.html#get-started-with-rstudio-server-open-source).)_
# MAGIC 
# MAGIC From the RStudio UI, you can import the SparkR package and set up a SparkR session to launch Spark jobs on your cluster.
# MAGIC 
# MAGIC ```r
# MAGIC library(SparkR)
# MAGIC sparkR.session()
# MAGIC ```
# MAGIC 
# MAGIC You can also attach the sparklyr package and set up a Spark connection.
# MAGIC 
# MAGIC ```r
# MAGIC SparkR::sparkR.session()
# MAGIC library(sparklyr)
# MAGIC sc <- spark_connect(method = "databricks")
# MAGIC ```
# MAGIC 
# MAGIC This will display the tables in the 'Data' tab of your Databricks Workspace in the 'Connections' window of RStudio.  Tables from the `default` database will be shown at first, but you can switch the database using `sparklyr::tbl_change_db()`.
# MAGIC 
# MAGIC ```r
# MAGIC ## Change from default database to non-default database
# MAGIC tbl_change_db(sc, "not_a_default_db")
# MAGIC ```
# MAGIC 
# MAGIC #### Persisting Work
# MAGIC 
# MAGIC Databricks clusters are treated as ephemeral computational resources - the default configuration is for the cluster to automatically terminate after 120 minutes of inactivity. This applies to your hosted instance of RStudio as well.  To persist your work you'll need to use [DBFS](https://github.com/marygracemoesta/R-User-Guide/blob/master/Getting_Started/DBFS.md) or integrate with version control.  
# MAGIC 
# MAGIC By default, the working directory in RStudio will be on the driver node.  To change it to a path on DBFS, simply set it with `setwd()`.
# MAGIC 
# MAGIC ```r
# MAGIC setwd("/dbfs/my_folder_that_will_persist")
# MAGIC ```
# MAGIC 
# MAGIC You can also access DBFS in the File Explorer.  Click on the `...` all the way to the right and enter `/dbfs/` at the prompt.
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/rstudio-gotofolder.png?raw=true" height = 250 width = 450>
# MAGIC 
# MAGIC Then the contents of DBFS will be available:
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/file_explorer_rstudio_dbfs.png?raw=true" height=200 width=450>
# MAGIC 
# MAGIC You can store RStudio Projects on DBFS and any other arbitrary file.  When your cluster is terminated at the end of your session, the work will be there for you when you return.
# MAGIC 
# MAGIC ##### Git Integration
# MAGIC An alternative approach is to sync your project with version control using the built-in support from RStudio.  This will persist your work from session to session as well.
# MAGIC 
# MAGIC ### RStudio Desktop with Databricks Connect
# MAGIC Databricks Connect is a library that allows users to remotely access Spark on Databricks clusters from their local machine.  At a high level the architecture looks like this:
# MAGIC 
# MAGIC <img src="https://spark.rstudio.com/images/deployment/databricks/rstudio-databricks-remote.png">
# MAGIC 
# MAGIC ##### Setup
# MAGIC Documentation can be found [here](https://docs.databricks.com/dev-tools/databricks-connect.html#databricks-connect), but essentially you will install the client library locally, configure the cluster on Databricks, and then authenticate with a token.  At that point you'll be able to connect to Spark on Databricks  from your local RStudio instance and develop freely.
# MAGIC 
# MAGIC ##### Gotchas 
# MAGIC Something important to note when using the Rstudio integrations:
# MAGIC - Loss of notebook functionality: magic commands that work in Databricks notebooks, do not work within Rstudio
# MAGIC - `dbutils` is not supported
# MAGIC ___
# MAGIC [To R User Guide](https://github.com/marygracemoesta/R-User-Guide#contents)