# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

hello <- function() {
  print("Hello, world!")
}

require("RPostgreSQL")
require(forecast)
require("tsoutliers")
require(ggplot2)
require("tsintermittent")
library("doParallel")
library("foreach")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "Atotech",
                 host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                 user = "aXialyze", password = "aXialyze0000")
df <- dbGetQuery(con, "SELECT materialghost, cluster, kilo, periods, order_periods, continuity, time_serie_category,
                 order_value FROM public.time_serie_categories_ghosts_cluster_month where time_serie_category = 'Continuous'" )
# Calculate the number of cores
no_cores <- detectCores() - 1

run_ph_cl_mm <- function(df, no_cores) {
    foreach(i=1:no_cores) %dopar%
    {
      require("RPostgreSQL")
      require(forecast)
      require("tsoutliers")
      require(ggplot2)
      require("tsintermittent")
      source('~/aXialyzefcstcontrol/R/main_functions.R')
      source('~/aXialyzefcstcontrol/R/phantom_region.R')
      drv <- dbDriver("PostgreSQL")
      con <- dbConnect(drv, dbname = "Atotech",
                       host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                       user = "aXialyze", password = "aXialyze0000")
      batchsize <- floor(nrow(df)/no_cores)
      startnr <- batchsize*i - batchsize + 1
      if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
      dfall <- df[startnr:endnr, ]
      level <- "phantom_region_Continous"
     apply(dfall, 1, f_ph_reg, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12)
    }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_ph_cl_mm(df, no_cores), error = function(e) print(e))
