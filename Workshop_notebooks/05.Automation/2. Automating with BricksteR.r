# Databricks notebook source
# MAGIC %md
# MAGIC ## Automating R Jobs on Databricks with bricksteR
# MAGIC 
# MAGIC While Databricks supports R users through interactive notebooks and a hosted instance of RStudio Server, as of June 2020 all R jobs must be Notebook Tasks.  This can make it cumbersome to convert R files into production jobs.  Sounds like a job for an R package...
# MAGIC 
# MAGIC `bricksteR` makes it easy to quickly turn .R and .Rmd files into automated jobs that run on Databricks by using the [Databricks REST API](https://docs.databricks.com/dev-tools/api/latest/).
# MAGIC 
# MAGIC Here are some highlights of what you can do with `bricksteR`:
# MAGIC 
# MAGIC -   *Import any .R or .Rmd file into the Databricks Workspace, from anywhere*
# MAGIC -   *Create and run a new job in a single function*
# MAGIC -   *Check on the status of a job run by name or id*
# MAGIC -   *Use JSON files or strings for job configurations*
# MAGIC 
# MAGIC **Table of Contents**
# MAGIC 
# MAGIC *  Installation & Authentication
# MAGIC *  Importing R Scripts to Databricks
# MAGIC *  Jobs
# MAGIC   *  Create and Run a Job
# MAGIC   *  View Jobs and Runs
# MAGIC   *  Reset a Job
# MAGIC   *  Delete a Job
# MAGIC *   Export from Databricks
# MAGIC 
# MAGIC #### Installation
# MAGIC 
# MAGIC The first thing you'll need to get started is to install the package from GitHub.

# COMMAND ----------

devtools::install_github("RafiKurlansik/bricksteR")
library(bricksteR)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Authentication
# MAGIC 
# MAGIC You will need an [Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#authentication) in order to authenticate with the Databricks REST API.
# MAGIC 
# MAGIC If you are working locally or using [DB Connect](https://docs.databricks.com/dev-tools/databricks-connect.html#databricks-connect), a good way to authenticate is to use a [.netrc](https://docs.databricks.com/dev-tools/api/latest/authentication.html#store-token-in-netrc-file-and-use-in-curl) file. By default, `bricksteR` will look for the `.netrc` file before checking for a token in the function call. If you don't already have one, create a .netrc file in your home directory that looks like this:
# MAGIC 
# MAGIC     machine <databricks-instance>
# MAGIC     login token
# MAGIC     password <personal-access-token-value>
# MAGIC 
# MAGIC You can also authenticate by [passing a token to Bearer authentication](https://docs.databricks.com/dev-tools/api/latest/authentication.html#pass-token-to-bearer-authentication). To be clear, **it is not recommended to store your credentials directly in your code**. If you are working on Databricks in a notebook, you can use `dbutils.secrets.get()` to avoid printing your token.
# MAGIC 
# MAGIC Finally, you will need to identify the workspace URL of your Databricks instance. On AWS this typically has the form `https://dbc-a64b1209f-d811.cloud.databricks.com`, or if you have a vanity URL it may look more like `https://mycompany.cloud.databricks.com`. On Azure it will have the form `https://eastus2.azuredatabricks.net`, or whichever region your instance is located in.

# COMMAND ----------

# Variables to authenticate
workspace <- "https://field-eng.cloud.databricks.com"

token <- dbutils.secrets.get("rk-rest-api", "brickster")

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Importing R Scripts to Databricks
# MAGIC 
# MAGIC A common use case is moving R scripts or R Markdown files from RStudio to Databricks to run as a notebook. We can automate this process using `import_to_workspace()`.  We have a script sitting on DBFS at the following path: `/dbfs/home/rafi.kurlansik@databricks.com/rstudio/mlflow_logging.R`.  Let's import this to the workspace as a notebook!

# COMMAND ----------

import_to_workspace(file = "/dbfs/home/rafi.kurlansik@databricks.com/rstudio/mlflow_logging.R",
                    notebook_path = "/Users/rafi.kurlansik@databricks.com/AnRMarkdownDoc",
                    workspace = workspace,
                    overwrite = 'true')
#> Status: 200
#> 
#> Object: /Users/rafi.kurlansik/RProjects/AnRMarkdownDoc.Rmd was added to the workspace at /Users/rafi.kurlansik@databricks.com/AnRMarkdownDoc
#> Response [https://demo.cloud.databricks.com/api/2.0/workspace/import]
#>   Date: 2019-12-18 05:26
#>   Status: 200
#>   Content-Type: application/json;charset=utf-8
#>   Size: 3 B
#> {}

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Jobs
# MAGIC 
# MAGIC ##### Create and Run a Job
# MAGIC 
# MAGIC Now that the file is in our workspace, we can create a job that will run it as a [Notebook Task](https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobsnotebooktask).
# MAGIC 
# MAGIC ``` r
# MAGIC new_job <- create_job(notebook_path = "/Users/rafi.kurlansik@databricks.com/AnRMarkDownDoc", 
# MAGIC                        name = "Beer Sales Forecasting Job",
# MAGIC                        job_config = "default",
# MAGIC                        workspace = workspace)
# MAGIC #> Status: 200
# MAGIC #> Job "Beer Sales Forecasting Job" created.
# MAGIC #> Job ID: 31123
# MAGIC 
# MAGIC job_id <- new_job$job_id
# MAGIC ```
# MAGIC 
# MAGIC Each job requires a `job_config`, which is a JSON file or JSON formatted string specifying (at a minimum) the task and type of infrastructure required. If no config is supplied, a 'default' cluster is created with 1 driver and 2 workers. Here's what that default configuration looks like:
# MAGIC 
# MAGIC ``` r
# MAGIC # Default JSON configs
# MAGIC  job_config <- paste0('{
# MAGIC   "name": ', name,'
# MAGIC   "new_cluster": {
# MAGIC     "spark_version": "5.3.x-scala2.11",
# MAGIC     "node_type_id": "r3.xlarge",
# MAGIC     "aws_attributes": {
# MAGIC       "availability": "ON_DEMAND"
# MAGIC     },
# MAGIC     "num_workers": 2
# MAGIC   },
# MAGIC   "email_notifications": {
# MAGIC     "on_start": [],
# MAGIC     "on_success": [],
# MAGIC     "on_failure": []
# MAGIC   },
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": ', notebook_path, '
# MAGIC   }
# MAGIC }')
# MAGIC ```
# MAGIC 
# MAGIC Where `name` is the name of your job and `notebook_path` is the path to the notebook in your workspace that will be run.
# MAGIC 
# MAGIC Creating a job by itself will not actually execute any code. If we want our beer forecast - and we do - then we'll need to actually run the job. We can run the job by id, or by name.
# MAGIC 
# MAGIC ``` r
# MAGIC # Specifying the job_id
# MAGIC run_job(job_id = job_id, workspace = workspace, token = NULL)
# MAGIC #> Status: 200
# MAGIC #> Run ID: 2393205
# MAGIC #> Number in Job: 1
# MAGIC 
# MAGIC # Specifying the job name
# MAGIC run_job(name = "Beer Sales Forecasting Job", workspace = workspace, token = NULL)
# MAGIC #> Job "Beer Sales Forecasting Job" found with 31123.
# MAGIC #> Status: 200
# MAGIC #> Run ID: 2393206
# MAGIC #> Number in Job: 2
# MAGIC ```
# MAGIC 
# MAGIC Running a job by name is convenient, but there's nothing stopping you from giving two jobs the same name on Databricks. The way `bricksteR` handles those conflicts today is by forcing you to use the unique Job ID or rename the job you want to run with a distinct moniker.
# MAGIC 
# MAGIC If you want to run a job immediately, one option is to use the `create_and_run_job()` function:
# MAGIC 
# MAGIC ``` r
# MAGIC # By specifying a local file we implicitly import it
# MAGIC running_new_job <- create_and_run_job(file = "/Users/rafi.kurlansik/agg_and_widen.R",
# MAGIC                                       name = "Lager Sales Forecasting Job",
# MAGIC                                       notebook_path = "/Users/rafi.kurlansik@databricks.com/agg_and_widen", 
# MAGIC                                       job_config = "default",
# MAGIC                                       workspace = workspace)
# MAGIC #> Job "Lager Sales Forecasting Job" successfully created...  
# MAGIC #> The Job ID is: 31124
# MAGIC #> 
# MAGIC #> Run successfully launched...  
# MAGIC #> The run_id is: 2393207
# MAGIC #> 
# MAGIC #> You can check the status of this run at: 
# MAGIC #>  https://demo.cloud.databricks.com#job/31124/run/1
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### View Jobs and Runs
# MAGIC 
# MAGIC There are two handy functions that will return a list of Jobs and their runs - `jobs_list()` and `runs_list()`. One of the objects returned is a concise R dataframe that allows for quick inspection.

# COMMAND ----------


``` r
jobs <- jobs_list(workspace = workspace)
#> Status: 200
#> Number of jobs: 629
beer_runs <- runs_list(name = "Lager Sales Forecasting Job", workspace = workspace)
#> Job "Lager Sales Forecasting Job" found with 31124.
#> Status: 200
#> Job ID: 31124
#> Number of runs: 1
#> 
#> Are there more runs to list? 
#>  FALSE

head(jobs$response_tidy)
#>   job_id          settings.name        created_time
#> 1  12017  test-job-dollar-signs 2018-12-04 15:55:01
#> 2  18676 Arun_Spark_Submit_Test 2019-08-28 18:32:49
#> 3  21042              test-seif 2019-10-30 05:06:32
#> 4   4441        Ad hoc Analysis 2017-08-29 20:21:20
#> 5  29355       Start ETL Stream 2019-12-04 18:26:40
#> 6  30837          Turo-Job-Test 2019-12-10 20:18:40
#>                 creator_user_name
#> 1     sid.murching@databricks.com
#> 2  arun.pamulapati@databricks.com
#> 3 seifeddine.saafi@databricks.com
#> 4            parag@databricks.com
#> 5              mwc@databricks.com
#> 6          lei.pan@databricks.com
head(beer_runs$response_tidy)
#>   runs.job_id runs.run_id        runs.creator_user_name
#> 1       31124     2393207 rafi.kurlansik@databricks.com
#>                                   runs.run_page_url
#> 1 https://demo.cloud.databricks.com#job/31124/run/1
```


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset a Job
# MAGIC 
# MAGIC If you need to update the config for a job, use `reset_job()`.
# MAGIC 
# MAGIC ``` r
# MAGIC # JSON string or file path can be passed
# MAGIC new_config <- "/configs/nightly_model_training.json"
# MAGIC 
# MAGIC reset_job(job_id = 31114, new_config, workspace = workspace)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete a Job
# MAGIC 
# MAGIC If you need to clean up a job from the workspace, use `delete_job()`.

# COMMAND ----------

delete_job(job_id = job_id, workspace = workspace)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Export from Databricks
# MAGIC 
# MAGIC To export notebooks from Databricks, use `export_from_workspace()`.
# MAGIC 
# MAGIC Passing a `filename` will save the exported data to your local file system. Otherwise it will simply be returned as part of the API response.

# COMMAND ----------

export_from_workspace(workspace_path = "/Users/rafi.kurlansik@databricks.com/AnRMarkdownDoc",
                      format = "SOURCE",
                      filename = "/Users/rafi.kurlansik/RProjects/anotherdoc.Rmd",
                      workspace = workspace)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC 
# MAGIC Questions or feedback? Feel free to contact me at <rafi.kurlansik@databricks.com>.

# COMMAND ----------

# Variables to authenticate
workspace <- "https://field-eng.cloud.databricks.com"
token <- dbutils.secrets.get("rk-rest-api", "brickster")

# Create the job
create_job(name = "Aggregation and Widening", 
           file = "/dbfs/rk/scripts/agg_and_widen.R",
           notebook_path = "/Users/rafi.kurlansik@databricks.com/R_on_Databricks/Aggregate and Widen",
           workspace = workspace,
           token = token)

# COMMAND ----------

run_job(3668, 
        workspace = workspace,
        token = token)

# COMMAND ----------

run_id <- 9477
status <- get_run_status(run_id, workspace, token)

display(status$response_df)

# COMMAND ----------

runs <- runs_list(job_id = 3668, 
                 workspace = workspace,
                 token = token)

# COMMAND ----------

runs