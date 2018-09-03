f_mat_cluster <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  phantom <- x[1]
  iorg_level <- x[2]


  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqtyvalue_per_date_inclmaterial_cluster($1,$2,$3,$4)"
  fcaccuracy <- extTryCatch(fcstMat_cluster(connection , phantom, iorg_level,  iquery, FALSE, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, iorg_level, iYYYY, fcperiod, sendfcserie, fcrun)
  # print(x[1])
}

fcstMat_cluster <- function( connection , Phantom, org_level, query,  intermittent, DateMask, yrfreq, status, todate) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, org_level, DateMask, todate))
  print (df_postgres)
  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)

  returnobject <- fcstgetAccuracy(myts, intermittent, status, yrfreq)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}


