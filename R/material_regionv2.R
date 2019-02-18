f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  phantom <- x[1]
  iorg_level <- x[2]
  region <- x[3]
  sma_only = FALSE
  if (x[6] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}
  print(region)
  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqtyvalue_per_date_inclmaterial_cluster_region($1,$2,$3,$4,$5)"
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, region, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, region, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, region, query,  intermittent, sma_only, DateMask, yrfreq, status, todate) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, org_level, DateMask, region, todate))
  print (df_postgres)
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


