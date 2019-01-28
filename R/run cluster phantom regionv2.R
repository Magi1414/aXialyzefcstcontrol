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
df <- dbGetQuery(con, "SELECT left(material,7), cluster,  sales_organization , sum(totalvolume) totalvolume,  'Continuous', 0
                 FROM public.sandop_selection_region s
                 where
                 not exists  (select material || geography from fcst_accuracy f where fcrun = (select fcrun_phantom from current_run)
                 and f.material = left(s.material,7) and left(geography,4) = sales_organization )
                 AND cluster = 'Germany'
                 group by left(material,7), cluster,  sales_organization
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
    source('~/aXialyzefcstcontrol/R/material_phantomv2.R')
    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname = "Atotech",
                     host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                     user = "aXialyze", password = "aXialyze0000")

    df2 <-  dbGetQuery(con, "SELECT fcrun, to_char(requested_deliv_date_to, 'yyyy-mm-dd') as requested_deliv_date_to , fcperiod FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun_phantom FROM current_run)" )


    batchsize <- floor(nrow(df)/no_cores)
    startnr <- batchsize*i - batchsize + 1
    if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
    dfall <- df[startnr:endnr, ]
    level <- "material_region_Continous"
    fcrun <- df2[,"fcrun"]
    todate <- df2[,"requested_deliv_date_to"]  #parameter for last date of history to take into account
    fcperiod <- df2[,"fcperiod"]
    apply(dfall, 1, f_mat_phant, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12, fcperiod, TRUE, fcrun, todate)
  }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_mat_cust_mm(df, no_cores), error = function(e) print(e))
