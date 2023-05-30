# Databricks notebook source
# MAGIC %md
# MAGIC #### 0.Define variables

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
rstudio_home_path = "/mnt/r_files_home"          # Mount home folder. See step 1!

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. R library utility functions
# MAGIC The script below contains:
# MAGIC * R functions that help users automatically install libraries in a "central" library location in the mounted folder (see step 1). 
# MAGIC   * e.g. instead of using `install.packages`, users should use `databricks.install.packages` which then automatically moves the installed packages to the centralised library folder.
# MAGIC * Secondly it automatically adds this centralised library folder to everyone's library paths.

# COMMAND ----------

rlib_install_path = rstudio_home_path + "/helper_functions/libs_install.r"

# COMMAND ----------

script ="""
############################################################################
#  R Library Management
#  Functions for command-line R library installation, setting search
#  path, managing libraries for different R runtime
############################################################################


setUserLibPath <- function(usrLibPath = "{0}/{1}"){{
  # Sets up user libraty path with DBFS fusion mount "{0}/{1}"
  #  and add into the R library search path
  #
  # Args:
  #   usrLibPath 
  #
  # Return:
  #   "{0}/{1}"
  usrLibPathVer = file.path(usrLibPath, getRversion())  #set the path matching R version
  if (file.exists(usrLibPathVer)==F) {{
    dir.create(usrLibPathVer, recursive=TRUE)
  }}
   
   
  search.path = .libPaths()
  if (usrLibPathVer %in% search.path) {{
    search.path = setdiff(search.path, usrLibPathVer)
  }}
   
  # adding user specified library path into search paths
  search.path = c(usrLibPathVer, search.path)
  .libPaths(search.path) 
   
  return(usrLibPathVer)
}}

removeUserLibPath <- function(usrLibPath){{
  # Remove user library path
  #  and add into the R library search path
  #
  # Args:
  #   path: Path to be removed from search path
  #
  # Return:
  #   search paths
   
  search.path = .libPaths()
  if (usrLibPath %in% search.path) {{
    search.path = setdiff(search.path, usrLibPath)
    .libPaths(search.path) 
  }}
  return(search.path)
}}
 
getUserLibPath <- function(){{
  #  Get user libraty path with DBFS fusion mount "{0}"
  # Args:
  #   None
  #
  # Return:
  #   "{0}/{1}" + R_runtime
  usrLibPath = "{0}/{1}"   
 
  return(file.path(usrLibPath, getRversion()) )
}}
 
 
databricks.install.packages <- function(pkgName, repo="http://cloud.r-project.org") {{
  # Install standard R packages into user library repo which is specified by function setUserLibPath()
  #
  # Args:
  #   pkgName:  Name of the R pacakges to be installed (case sensitive)
  #   repo: Optional. CRAN-like package repository
  # 
  # Return:
  #   tmpDir:  The temperary directory that holds the installed pacakge
   
  usrLibPath = setUserLibPath()
  tmpDir <- tempfile(pattern = "{1}")
  dir.create(tmpDir)
  install.packages(pkgName,repos=repo, dependencies=T, lib = tmpDir)
  system(paste0("cp -r ", tmpDir, "/* ", usrLibPath))
  removeUserLibPath(tmpDir)
  tmpDir
}}

databricks.install.github <- function(repo) {{
  # Install standard R packages into user library repo which is specified by function setUserLibPath()
  #
  # Args:
  #   pkgName:  Name of the github repo to be installed (case sensitive)
  # 
  # Return:
  #   tmpDir:  The temperary directory that holds the installed pacakge
  
  tmpDir <- tempfile(pattern = "{1}")
  dir.create(tmpDir)
  tmpPath = setUserLibPath(tmpDir)
  require('devtools')
  install_github(repo)
  usrLibPath = setUserLibPath()
  system(paste0("cp -r ", tmpPath, "/* ", usrLibPath))
  removeUserLibPath(tmpPath)
  tmpDir
}}
 
databricks.install_version <- function(pkgName, what.version, repo='http://cran.us.r-project.org') {{
  # Install a specific version of standard R packages into user library repo
  # which is specified by function setUserLibPath()
  #
  # Args:
  #   pkgName:  Name of the R pacakges to be installed (case sensitive)
  #   what.version:  The specific version of package to be installed
  #   repo: Optional. CRAN-like package repository
  # 
  # Return:
  #   tmpDir:  The temperary directory that holds the installed pacakge
  usrLibPath = setUserLibPath()
  tmpDir <- tempfile(pattern = "{1}")
  dir.create(tmpDir)
   
  if (!suppressWarnings(library('devtools',logical.return=TRUE))){{
      databricks.install.packages('devtools', repos=repo)
      require(devtools)
  }}
   
  if (!suppressWarnings(library('devtools',logical.return=TRUE))){{
      databricks.install.packages('withr', repos=repo)
      require(withr)
  }}
   
  withr::with_libpaths(new=tmpDir, install_version(pkgName, version=what.version, repos=repo, dependencies=TRUE))
  system(paste0("cp -r ", tmpDir, "/* ", usrLibPath))
  tmpDir
}}
setUserLibPath()   # add {0}/{1} into library search path
""".format("/dbfs" + rstudio_home_path,"rlib")
dbutils.fs.put("dbfs:" + rlib_install_path, script, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. R configuration script (`.Rprofile/Rprofile.site`)
# MAGIC Below is a script that we can add to `.Rprofile` or `Rprofile.site`. `Rprofile.site` is used when we want to apply the script to ALL Rstudio users, while `.Rprofile` is tied to a specific user. More on `.Rprofile` and `Rprofile.site` is explained [here](https://support.rstudio.com/hc/en-us/articles/360047157094-Managing-R-with-Rprofile-Renviron-Rprofile-site-Renviron-site-rsession-conf-and-repos-conf)).
# MAGIC 
# MAGIC The most important parts that are added to the below `.Rprofile`
# MAGIC * Setting a default working directory to `dbfs`. This will ensure that RStudio users do not save their files on the cluster driver, which could cause the loss of files.
# MAGIC * Running the library script that contains the R library utility functions

# COMMAND ----------

r_config_path = rstudio_home_path + "/init/config.r"

# COMMAND ----------

script = """
print('Executing custom site Rprofile...')
# setup environment variables
Sys.setenv("GITHUB_PAT" = "MY_PAT")

# warn on partial matches
options(warnPartialMatchAttr = TRUE,
        warnPartialMatchDollar = TRUE,
        warnPartialMatchArgs = TRUE)

# enable autocompletions for package names in
# `require()`, `library()`
utils::rc.settings(ipck = TRUE)

# set working directory
setwd("{0}")

# warnings are errors
options(warn = 2)

# fancy quotes are annoying and lead to
# 'copy + paste' bugs / frustrations
options(useFancyQuotes = FALSE)

# source library install code 
source('{1}')
""".format("/dbfs" + rstudio_home_path, "/dbfs" + rlib_install_path)
dbutils.fs.put("dbfs:" + r_config_path, script, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Using bash init scripts to configure .Rprofile/Rprofile.site
# MAGIC Now that we defined the R initialisation script in step 2, we must add this to either `.Rprofile` or `Rprofile.site` to make sure R initialises this script. We do this by making use of cluster init scripts, which are defined below. There are two cluster init scripts: One is used to set up the `.Rprofile` the other for `Rprofile.site` . It is left to the user to choose which one is most appropriate. For R configurations that should apply to all users, going with `Rprofile.site` is recommended.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### option 1: Init script for .Rprofile
# MAGIC The first one is for creating the `.Rprofile` R initialisation script, which is tied to a specific user. As such, it must be added to the `/home/<user_email/.Rprofile` directory in the driver. Note that a for loop could be used in the init script to add the `.Rprofile` script to multiple user directories.

# COMMAND ----------

rprofile_init_script_location = "dbfs:/tmp/{0}/init/rstudio_init_script.sh".format(username) # Must be located in root bucket

# COMMAND ----------

rprofile_init_script = """
#!/bin/bash
set -euxo pipefail

if [[ $DB_IS_DRIVER = "TRUE" ]]; then  
  
  USER="{0}"
  mkdir -p /home/$USER
  chmod 777 /home/$USER
  echo "$(cat {1})" >> /home/$USER/.Rprofile
fi
""".format(username, "/dbfs" + r_config_path)
dbutils.fs.put(rprofile_init_script_location,rprofile_init_script, True)

# COMMAND ----------

rprofile_init_script_location ## Add this location to the init script location when configuring your RStudio cluster!

# COMMAND ----------

# MAGIC %md
# MAGIC ##### option 2: init script for Rprofile.site
# MAGIC Below you will find the init script for configuring `Rprofile.site`. What this script essentially does, is it 

# COMMAND ----------

rprofilesite_init_script_location = "dbfs:/tmp/{0}/init/rprofilesite_init_script.sh".format(username)

# COMMAND ----------

rprofile_site_init_script = """
#!/bin/bash
set -euxo pipefail

if [[ $DB_IS_DRIVER = "TRUE" ]]; then  
  
  chmod 777 /usr/lib/R/etc/Rprofile.site
  echo "$(cat {1})" >> /usr/lib/R/etc/Rprofile.site
fi
""".format(username, "/dbfs" + r_config_path)
dbutils.fs.put(rprofilesite_init_script_location,rprofile_site_init_script, True)

# COMMAND ----------

# Add below path to init scripts when configuring your RStudio cluster!
rprofilesite_init_script_location