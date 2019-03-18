f_mat_regi_multi <- function(x, connection, ilevel, iYYYY, ifreq,  sendfcserie) {
  phantom <- x[1]
  region <- x[2]
  sma_only = FALSE

  df2 <-  dbGetQuery(connection, "SELECT fcrun, to_char(requested_deliv_date_to, 'yyyy-mm-dd') as
                     requested_deliv_date_to , fcperiod FROM public.parameter_sets ps where ps.fcrun = $1" ,c(x[3]))
  fcrun <- df2[,"fcrun"]
  todate <- df2[,"requested_deliv_date_to"]  #parameter for last date of history to take into account
  fcperiod <- df2[,"fcperiod"]

  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqtyvalue_per_date_inclmaterial_cluster_region_multi($1,$2,$3,$4,$5)"
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, region, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate, fcrun))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, region, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, region, query,  intermittent, sma_only, DateMask, yrfreq, status, todate, fcrun) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, DateMask, region, todate, fcrun))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)
  ##myts <- myts[cumsum(myts)!=0]
  returnobject <- fcstgetAccuracy(myts, intermittent, status, yrfreq,sma_only)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}


