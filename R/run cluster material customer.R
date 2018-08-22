#

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
df <- dbGetQuery(con, "SELECT material, cluster,  customer_code, totalvolume,  ts_categorie
                 FROM public.sandop_selection
                 where not material || customer_code in (select material || left(geography,10) from fcst_accuracy where fcrun = '20180822-1')
                 order by totalvolume desc limit 50" )

#other option df <- dbGetQuery(con, "SELECT material, cluster, lpad(customer_code,10,'0') customer_code, totalvolume,  ts_categorie
#FROM public.sandop_selection
#where not material || lpad(customer_code,10,'0') in (select material || left(geography,10) from fcst_accuracy)
#order by totalvolume desc" )
# Calculate the number of cores
no_cores <- detectCores() - 1

run_mat_cust_mm <- function(df, no_cores) {
    foreach(i=1:no_cores) %dopar%
    {
      require("RPostgreSQL")
      require(forecast)
      require("tsoutliers")
      require(ggplot2)
      require("tsintermittent")
      source('~/aXialyzefcstcontrol/R/main_functions.R')
      source('~/aXialyzefcstcontrol/R/material_customer.R')
      drv <- dbDriver("PostgreSQL")
      con <- dbConnect(drv, dbname = "Atotech",
                       host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                       user = "aXialyze", password = "aXialyze0000")
      batchsize <- floor(nrow(df)/no_cores)
      startnr <- batchsize*i - batchsize + 1
      if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
      dfall <- df[startnr:endnr, ]
      level <- "material_customer_Continous"
      fcrun <- "20180822-1"
     apply(dfall, 1, f_mat_cust, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12, "07.2018", TRUE, fcrun)
    }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_mat_cust_mm(df, no_cores), error = function(e) print(e))
