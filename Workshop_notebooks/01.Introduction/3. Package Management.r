# Databricks notebook source
# MAGIC %md
# MAGIC # Package Management on Databricks
# MAGIC 
# MAGIC Databricks supports a variety of options for installing and managing new, old, and custom R packages.  In this chapter we'll begin by providing examples of the basic approaches, then progress into more advanced options.
# MAGIC 
# MAGIC **Contents**
# MAGIC 
# MAGIC * Installing Packages
# MAGIC   * Notebook Scope
# MAGIC   * Cluster Scope
# MAGIC   * Older Package Versions
# MAGIC   * Custom Packages
# MAGIC * Faster Package Loads
# MAGIC * Databricks Container Services
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ## Installing Packages
# MAGIC 
# MAGIC ### Notebook Scope
# MAGIC 
# MAGIC At the most basic level, you can install R packages in your notebooks and RStudio scripts using the familiar `install.packages()` function. 
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/installpackages.png?raw=true">
# MAGIC 
# MAGIC This will install the package on the driver node **only**.  
# MAGIC 
# MAGIC ### Cluster Scope
# MAGIC 
# MAGIC Under the 'Libraries' tab in the Clusters UI you can attach packages to the cluster.  
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/attach_library_clusters_ui.png?raw=true">
# MAGIC 
# MAGIC Each time the cluster is started these packages will be installed on *both* driver and worker nodes.  This is important for when you want to perform user defined functions with `SparkR` or `sparklyr`.
# MAGIC 
# MAGIC ### Older Package Versions
# MAGIC 
# MAGIC Each release of Databricks Runtime includes a set of pre-installed popular R packages.  These are typically the latest stable versions but sometimes installing the latest version of a package can break your code.  
# MAGIC 
# MAGIC For instance, in [DBR 5.5, the version of `dplyr` is 0.8.0.1](https://docs.databricks.com/release-notes/runtime/5.5.html#installed-r-libraries) but what if your code won't run unless the installed version is 0.7.4?  How do you go about installing an older version of a package on Databricks?
# MAGIC 
# MAGIC At the notebook or script level there are [several ways](https://support.rstudio.com/hc/en-us/articles/219949047-Installing-older-versions-of-packages) to install older versions of packages.  One quick way is the `install_version()` function from `devtools`.
# MAGIC 
# MAGIC ```R
# MAGIC require(devtools)
# MAGIC install_version("dplyr", version = "0.7.4", repos = "http://cran.us.r-project.org")
# MAGIC ```
# MAGIC 
# MAGIC To install an older package at the cluster scope, use a snapshot from the [Microsoft R Application Network](https://mran.microsoft.com/) (MRAN).  MRAN saves the contents of CRAN on a daily basis and stores them as snapshots.  Packages pulled from a specific date will contain the latest version of the package available on that date.  For version 0.7.4 of `dplyr`, we would have to go back to the snapshot from December 19, 2015 and use that URL as the repository in the Cluster UI.
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/install_version.png?raw=true">
# MAGIC 
# MAGIC Checking the package version on our cluster after installing from this MRAN snapshot, we see the correct version:
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/install_dplyr.png?raw=true">
# MAGIC 
# MAGIC In this way we can achieve greater customization of the packages on our cluster, even overwriting the versions pre-installed with Databricks Runtime.
# MAGIC 
# MAGIC ### Custom Packages
# MAGIC 
# MAGIC To install a custom package on Databricks, first [build](https://kbroman.org/pkg_primer/pages/build.html) your package from the command line locally or [using RStudio](http://r-pkgs.had.co.nz/package.html).  Next, use the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html) to copy the file to DBFS:
# MAGIC 
# MAGIC ```bash
# MAGIC databricks fs cp /local_path_to_package/custom_package.tar.gz dbfs:/path_to_package/
# MAGIC ```
# MAGIC 
# MAGIC Once you have the `tar.gz` file on DBFS, you can install the package using `install.packages()`.
# MAGIC 
# MAGIC ```R
# MAGIC ## In R
# MAGIC install.packages("/dbfs/path_to_package/custom_package.tar.gz", type = "source", repos = NULL)
# MAGIC ```
# MAGIC This can also be done in a `%sh` cell:
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/install_custom_sh.png?raw=true">
# MAGIC 
# MAGIC If you want to install the custom package on each node in the cluster you will need to use an [init script](linktocome).
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ## Faster Package Loads
# MAGIC 
# MAGIC _...you will be fastest if you avoid doing the work in the first place._ [[1]](http://dirk.eddelbuettel.com/blog/2017/11/27/#011_faster_package_installation_one)
# MAGIC 
# MAGIC Attaching dozens of packages can significantly extend the time it takes for your cluster to come online or for your job to complete.  To understand why this happens, we need to take a closer look at where packages come from.  
# MAGIC 
# MAGIC ### What is slowing us down?
# MAGIC 
# MAGIC CRAN stores packages in 3 different formats: Mac, Windows, and source.  
# MAGIC 
# MAGIC The default behavior of `install.packages()` is to download the package binaries for your operating system, _if they are available_.  If they aren't available R will instead download the package source files from CRAN in `packageName.tar.gz`format.  
# MAGIC 
# MAGIC Binaries can be installed into your library directly, while source files need to be compiled first.  Windows and Mac users will usually be able to skip compiling, shortening overall installation time.  Linux users will almost always have to compile from source.  This extra time spent compiling adds up quickly when installing many packages.
# MAGIC 
# MAGIC There are therefore two problems to overcome with regard to performance.  First, since Databricks Runtime uses Linux you are always installing packages on CRAN from source.  Second, a Databricks cluster terminates when not actively in use, taking all of the installed packages down with it.  Every time we spin those machines up, we start from scratch and perform the work of downloading, compiling, and installing all over again. 
# MAGIC 
# MAGIC ### Getting the Best Performance on Databricks
# MAGIC 
# MAGIC To get better performance we need to avoid doing all that work in the first place!  We can accomplish this by ultimately persisting our installed packages in a library on DBFS. 
# MAGIC 
# MAGIC ### Building a Library on DBFS
# MAGIC 
# MAGIC All packages are installed into a _library_, which is located on a path in the file system.  You can check the what directories are recognized by R as libraries with `.libPaths()`.  
# MAGIC 
# MAGIC <img src="https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/images/defaul_libpaths.png?raw=true"> 
# MAGIC 
# MAGIC When you call `library(dplyr)` R will search for the package in the libraries listed under `.libPaths()`, starting at the first path and if the package isn't found continue searching through the rest of the directories in order.  You can add and remove paths from `.libPaths()`, effectively telling R where to look for packages.   Let's create a directory on the driver to install packages into and add it to the list of libraries.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ```bash
# MAGIC %sh
# MAGIC mkdir /usr/lib/R/proj-lib-test
# MAGIC ```
# MAGIC 
# MAGIC ```R
# MAGIC ## Append our library to the list
# MAGIC .libPaths(c("/usr/lib/R/proj-lib-test", .libPaths()))
# MAGIC ```
# MAGIC 
# MAGIC Now every package we install will write to that directory on the driver.  Once you have the packages and versions you want in that directory, **copy them to DBFS to persist.**
# MAGIC 
# MAGIC ```R
# MAGIC ## Copy to DBFS
# MAGIC system("cp -R /usr/lib/R/proj-lib-test /dbfs/path_to_r_library", intern = T)
# MAGIC ```
# MAGIC Now when the cluster is terminated, the packages will remain.  
# MAGIC 
# MAGIC See [this page](https://github.com/marygracemoesta/R-User-Guide/blob/master/Developing_on_Databricks/notebooks/faster_package_loads.md) for a more thorough example. 
# MAGIC 
# MAGIC ### Setting the Library Path
# MAGIC 
# MAGIC Assuming you have installed your desired packages into a library on DBFS, you can begin using them with one command. 
# MAGIC 
# MAGIC ```R
# MAGIC ## Add library to search path
# MAGIC .libPaths(c("/dbfs/path_to_r_library", .libPaths()))
# MAGIC 
# MAGIC ## Load package, no need to install again!
# MAGIC library(custom_package)
# MAGIC ```
# MAGIC 
# MAGIC If you need these libraries to be available to each worker, you can use an [init script](https://github.com/marygracemoesta/R-User-Guide/edit/master/Developing_on_Databricks/Customizing.md) or simply include it in your closure when running [user defined functions](linktocome).
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ## Databricks Container Services
# MAGIC 
# MAGIC [Databricks Container Services](https://docs.databricks.com/clusters/custom-containers.html) (DCS) lets you specify a Docker image to run across your cluster.  This can be built off of the base Databricks Runtime image, or it can be entirely customized to your specifications.  DCS will give you the ultimate control and flexibility when it comes to locking down packages and their versions in an execution environment.  
# MAGIC 
# MAGIC Here is an example R Dockerfile pulled from [the official git repo for DCS](https://github.com/databricks/containers) that grabs the latest version of R from RStudio:
# MAGIC 
# MAGIC ```
# MAGIC FROM databricksruntime/minimal:latest
# MAGIC 
# MAGIC # Ubuntu 16.04.3 LTS installs R version 3.2.3 by default. This is fairly out dated.
# MAGIC # We add RStudio's debian source to install the latest r-base version (3.6.0)
# MAGIC # We are using the more secure long form of pgp key ID of marutter@gmail.com
# MAGIC # based on these instructions (avoiding firewall issue for some users):
# MAGIC # https://cran.rstudio.com/bin/linux/ubuntu/#secure-apt
# MAGIC RUN apt-get update \
# MAGIC   && apt-get install --yes software-properties-common apt-transport-https \
# MAGIC   && gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
# MAGIC   && gpg -a --export E298A3A825C0D65DFD57CBB651716619E084DAB9 | sudo apt-key add - \
# MAGIC   && add-apt-repository 'deb [arch=amd64,i386] https://cran.rstudio.com/bin/linux/ubuntu xenial-cran35/' \
# MAGIC   && apt-get update \
# MAGIC   && apt-get install --yes \
# MAGIC     libssl-dev \
# MAGIC     r-base \
# MAGIC     r-base-dev \
# MAGIC   && add-apt-repository -r 'deb [arch=amd64,i386] https://cran.rstudio.com/bin/linux/ubuntu xenial-cran35/' \
# MAGIC   && apt-key del E298A3A825C0D65DFD57CBB651716619E084DAB9 \
# MAGIC   && apt-get clean \
# MAGIC   && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
# MAGIC 
# MAGIC # hwriterPlus is used by Databricks to display output in notebook cells
# MAGIC # Rserve allows Spark to communicate with a local R process to run R code
# MAGIC RUN R -e "install.packages('hwriterPlus', repos='https://mran.revolutionanalytics.com/snapshot/2017-02-26')" \
# MAGIC  && R -e "install.packages('Rserve', repos='http://rforge.net/')"
# MAGIC  ```
# MAGIC  
# MAGIC Notice how the last two lines illustrate how to incorporate package installation in the image, including setting the version of the package.
# MAGIC ___
# MAGIC [To R User Guide](https://github.com/marygracemoesta/R-User-Guide#contents)

# COMMAND ----------

