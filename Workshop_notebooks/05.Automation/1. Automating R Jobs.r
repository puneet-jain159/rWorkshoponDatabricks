# Databricks notebook source
# MAGIC %md
# MAGIC #### Automating R Jobs using the Databricks REST API
# MAGIC In this notebook we show how you can automate R scripts and R notebooks using the Databricks REST API. Note that jobs can also simply be created by making use of the Jobs UI. Automating a R script requires three steps:
# MAGIC * Authentication: We will need an access token that will be used to authenticate to the Databricks REST API
# MAGIC * Importing scripts: If we want to automate a R script, we will first have to import it to the Databricks workspace as a notebook.
# MAGIC * Creating and running the job: Once we have a R notebook, we can create a job around it and subsequently kick it off.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Authentication
# MAGIC 
# MAGIC You will need an [Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#authentication) in order to authenticate with the Databricks REST API.
# MAGIC 
# MAGIC We can pass this token [using Bearer authentication](https://docs.databricks.com/dev-tools/api/latest/authentication.html#pass-token-to-bearer-authentication). To be clear, **it is not recommended to store your credentials directly in your code**. If you are working on Databricks in a notebook, you can use `dbutils.secrets.get()` to avoid printing your token.
# MAGIC 
# MAGIC Finally, you will need to identify the workspace URL of your Databricks instance. On AWS this typically has the form `https://dbc-a64b1209f-d811.cloud.databricks.com`, or if you have a vanity URL it may look more like `https://mycompany.cloud.databricks.com`. On Azure it will have the form `https://eastus2.azuredatabricks.net`, or whichever region your instance is located in.

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Importing R Scripts to Databricks
# MAGIC 
# MAGIC A common use case is moving R scripts or R Markdown files from RStudio to Databricks to run as a notebook. We can automate this process using the Databricks REST API.  We have a script sitting on DBFS at the following path: ``.  Let's import this to the workspace as a notebook!

# COMMAND ----------

file = "/dbfs/Users/carsten.thone@databricks.com/rstudio_scripts/exampleScript.R"
notebook_path = "/Users/carsten.thone@databricks.com/exampleRscript2"
workspace <- "https://field-eng.cloud.databricks.com"
token <- "dapi059dbce1361d2023fddb44815202eb93" # Fill in your own PAT

# COMMAND ----------

import_to_workspace <- function(file,notebook_path,workspace,token) {
  
  files <- list(
    path = notebook_path,
    language = "R",
    content = httr::upload_file(file),
    overwrite = "true"
  )
  
  headers <- c(
  Authorization = paste("Bearer", token))
  
  url = paste0(workspace, "/api/2.0/workspace/import")
  
  
  res <- httr::POST(url = paste0(workspace, "/api/2.0/workspace/import"),
                  httr::add_headers(.headers = headers),
                  body = files)
  
  return(res)
}

# COMMAND ----------

import_to_workspace(file, notebook_path, workspace, token)

# COMMAND ----------

# MAGIC %md
# MAGIC ___
# MAGIC #### Jobs
# MAGIC 
# MAGIC ##### Create and Run a Job
# MAGIC 
# MAGIC Now that the file is in our workspace, we can use the REST API to create a job that will run it as a [Notebook Task](https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobsnotebooktask).
# MAGIC 
# MAGIC ``` r
# MAGIC create_job <- function(job_config, token) {
# MAGIC   
# MAGIC   res <- httr::POST(url = paste0(workspace, "/api/2.0/jobs/create"),
# MAGIC                       httr::add_headers(.headers = headers),
# MAGIC                       httr::content_type_json(),
# MAGIC                       body = job_config)
# MAGIC   
# MAGIC   return(res)
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC Each job requires a `job_config`, which is a JSON file or JSON formatted string specifying (at a minimum) the task and type of infrastructure required. Here's what that configuration looks like that we will use:
# MAGIC 
# MAGIC ``` r
# MAGIC # Default JSON configs
# MAGIC job_name = "example_job"
# MAGIC 
# MAGIC example_job_config <- sprintf('{
# MAGIC   "name": "%s",
# MAGIC   "new_cluster": {
# MAGIC     "spark_version": "7.3.x-scala2.12",
# MAGIC     "node_type_id": "i3.xlarge",
# MAGIC     "num_workers": 2
# MAGIC   },
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "%s"
# MAGIC   }
# MAGIC }', job_name, notebook_path)
# MAGIC ```
# MAGIC 
# MAGIC Where `name` is the name of your job and `notebook_path` is the path to the notebook in your workspace that will be run.
# MAGIC 
# MAGIC Creating a job by itself will not actually execute any code. then we'll need to actually run the job. We can use the `2.0/jobs/run-now` endpoint to run it now. We can run the job by job id, or by job name.
# MAGIC 
# MAGIC ``` r
# MAGIC # Specifying the job_id
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Running a job by name is convenient, but there's nothing stopping you from giving two jobs the same name on Databricks, so it's safest to run it using a job ID.

# COMMAND ----------

job_name = "example_job_2"

example_job_config <- sprintf('{
  "name": "%s",
  "new_cluster": {
    "spark_version": "7.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "notebook_task": {
    "notebook_path": "%s"
  }
}', job_name, notebook_path)

create_job <- function(job_config, token) {
  
  headers <- c(
  Authorization = paste("Bearer", token))
  
  res <- httr::POST(url = paste0(workspace, "/api/2.0/jobs/create"),
                      httr::add_headers(.headers = headers),
                      httr::content_type_json(),
                      body = job_config)
  
  job_id = jsonlite::fromJSON(rawToChar(res$content))[[1]]
  
  # Return response
  reslist <- list(response = res,
                  job_id = job_id)
  
  return(reslist)
}

# COMMAND ----------

res <- create_job(example_job_config, token)

# COMMAND ----------

job_id <- res$job_id

# COMMAND ----------

job_id

# COMMAND ----------

run_job <- function(job_id, token) {
  
  headers <- c(
      Authorization = paste("Bearer", token)
  )
  
  # Make request
  res <- httr::POST(url = paste0(workspace, "/api/2.0/jobs/run-now"),
                    httr::add_headers(.headers = headers),
                    httr::content_type_json(),
                    body = sprintf('{"job_id": %s}', job_id))
  
  return(res)
}

# COMMAND ----------

sprintf('{job_id: %s}', job_id)

# COMMAND ----------

res <- run_job(job_id, token=token)

# COMMAND ----------

res

# COMMAND ----------

