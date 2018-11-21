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
df <- dbGetQuery(con, "SELECT material, cluster,  sales_organization , totalvolume,  ts_categorie, case when sma_only then 1 else 0 end
                 FROM public.sandop_selection_region s
                 where
                 not exists  (select material || geography from fcst_accuracy f where fcrun = '20181104-2'
                 and f.material = s.material and left(geography,4) = sales_organization )
                 AND prod_group ilike '1%' and not prod_group = '106' and cluster = 'China' and totalvolume > 0
                 order by totalvolume desc" )

#other option df <- dbGetQuery(con, "SELECT material, cluster, lpad(custdfomer_code,10,'0') customer_code, totalvolume,  ts_categorie
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
      source('~/aXialyzefcstcontrol/R/material_regionv2.R')
      drv <- dbDriver("PostgreSQL")
      con <- dbConnect(drv, dbname = "Atotech",
                       host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                       user = "aXialyze", password = "aXialyze0000")
      batchsize <- floor(nrow(df)/no_cores)
      startnr <- batchsize*i - batchsize + 1
      if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
      dfall <- df[startnr:endnr, ]
      level <- "material_region_Continous"
      fcrun <- "20181104-2"
      todate <- "2018-10-31"  #parameter for last date of history to take into account
     apply(dfall, 1, f_mat_regi, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12, "10.2018", TRUE, fcrun, todate)
    }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_mat_cust_mm(df, no_cores), error = function(e) print(e))
