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
df <- dbGetQuery(con, "SELECT distinct continent ,
                        	sales_org|| ' - ' || business_grp || ' - ' || scenario || ' - ' || unit  material,
                        	0 sma_only, 0 revenue, 0 cum_sum_percentage,
                        	to_char(ss,'YYYY-MM') requested_deliv_date_to, to_char(ss + '1 month' ::interval,'MM.YYYY')fcperiod ,business_grp, scenario, unit
                                                  from generate_series(
                        		(select requested_deliv_date_to
                        		FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun FROM current_run)),
                        		(select todate
                        		FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun FROM current_run) ),'1 month') ss
                                            left join revenue_vol_selection_continent v on v.bw_date= to_char(ss,'YYYYMM')
                            where
                            not exists  (select material || geography from fcst_accuracy f where fcrun = (select fcrun from current_run)
                                             and left(f.material,4) = v.sales_org and geography = continent
                                             and f.material ilike '%'|| business_grp||'%'
                                             and f.material ilike '%'|| unit||'%'
                                             and f.material ilike '%'|| scenario||'%' )
                    order by continent,material,requested_deliv_date_to

                 " )


#other option df <- dbGetQuery(con, "SELECT material, cluster, lpad(custdfomer_code,10,'0') customer_code, totalvolume,  ts_categorie
#FROM public.sandop_selection
#where not material || lpad(customer_code,10,'0') in (select material || left(geography,10) from fcst_accuracy)
#order by totalvolume desc" )
# Calculate the number of cores
no_cores <- min(detectCores() - 1, nrow(df))

run_mat_cust_mm <- function(df, no_cores) {
  foreach(i=1:no_cores) %dopar%
  {
    require("RPostgreSQL")
    require(forecast)
    require("tsoutliers")
    require(ggplot2)
    require("tsintermittent")
    source('~/R/aXialyzefcstcontrol/R/main_functions.R')
    source('~/R/aXialyzefcstcontrol/R/bw revenue_volume/revenue_continent.R')

    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv, dbname = "Atotech",
                     host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                     user = "aXialyze", password = "aXialyze0000")

    df2 <-  dbGetQuery(con, "SELECT fcrun, to_char(requested_deliv_date_to, 'yyyy-mm-dd') as requested_deliv_date_to , fcperiod
                       FROM public.parameter_sets ps where ps.fcrun = (SeLECT fcrun FROM current_run)" )


    batchsize <- floor(nrow(df)/no_cores)
    startnr <- batchsize*i - batchsize + 1
    if(batchsize*(i+1) > nrow(df)){endnr <- nrow(df) }else {endnr <-  batchsize*i}
    dfall <- df[startnr:endnr, ]
    print(dfall)
    level <- "material_region_Continous"
    fcrun <- df2[,"fcrun"]
    apply(dfall, 1, f_mat_regi, connection = con, ilevel = level,  iYYYY = "YYYY-MM"  ,ifreq = 12,  TRUE, fcrun)
  }}
# Initiate cluster
cl <- makeCluster(no_cores)
doParallel::registerDoParallel(cl)
tryCatch(run_mat_cust_mm(df, no_cores), error = function(e) print(e))
