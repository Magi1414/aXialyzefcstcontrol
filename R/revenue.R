f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  print(x)
  phantom <- x[2]
  iorg_level <- 'topdown'
  region <- x[1]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}  
  print(region)
  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  

  iquery <- "SELECT to_date(month,'YYYYMM') requested_deliv_date_to, revenue litre
                from revenue_bw_v where   month  <=  to_char(to_date($1,'YYYY-MM-DD'),'YYYYMM') and country = $2 order by 1 asc "
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, region, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  # fcaccuracy <- extTryCatch(fcstMat_region(con , "phantom", "iorg_level", "region", iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, region, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, region, query,  intermittent, sma_only, DateMask, yrfreq, status, todate) {
  
  
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c( todate,region))#Phantom, org_level, DateMask, region,
  # df_postgres <- RPostgreSQL::dbGetQuery(con,  iquery, c( todate,region))#Phantom, org_level, DateMask, region,
  print (df_postgres)
  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  # myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = ifreq)
  ##return (myts)
  
  returnobject <- fcstgetAccuracy(myts, intermittent, status, yrfreq,sma_only)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"
  
  return (returnobject)
  
}


