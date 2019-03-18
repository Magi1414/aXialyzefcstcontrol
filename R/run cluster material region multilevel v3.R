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
df <- dbGetQuery(con, "SELECT distinct material,  salesorg,to_char(ss,'YYYY-MM-DD') requested_deliv_date_to, to_char(ss + '1 month' ::interval,'MM.YYYY') fcperiod
                 FROM generate_series(
                 (select requested_deliv_date_to
                 FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun FROM current_run)),
                 (select todate
                 FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun FROM current_run) ),'1 month') ss

                 left join      public.material_planninglevel m
                 on planninglevel = 'material region' and salesorg ilike '8%'
                 where not exists (select 1 from fcst_accuracy f where f.fcrun = (SeLECT fcrun FROM current_run) and m.material = f.material and geography = salesorg and fcperiod =to_char(ss + '1 month' ::interval,'MM.YYYY') )
limit 50
;" )

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
    require("uuid")
    source('~/R/aXialyzefcstcontrol/R/main_functions.R')
    source('~/R/aXialyzefcstcontrol/R/material_region multi v3.R')
    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname = "Atotech",
                     host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                     user = "aXialyze", password = "aXialyze0000")
    batchsize <- floor(nrow(df)/no_cores)
    startnr <- batchsize*i - batchsize + 1
    if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
    dfall <- df[startnr:endnr, ]
    level <- "material_region_multi"

    apply(dfall, 1, f_mat_regi_multi, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12,  TRUE)
  }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_mat_cust_mm(df, no_cores), error = function(e) print(e))
