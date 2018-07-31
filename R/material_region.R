f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq) {
  phantom <- x[1]
  iorg_level <- x[2]
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqtyvalue_per_date_inclmaterial_cluster($1,$2,$3)"
  fcaccuracy <- extTryCatch(fcstMat_regi(connection , phantom, iorg_level, iquery, FALSE, iYYYY, ifreq , status_message))
  print(x[1])
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, iorg_level, iYYYY)
  print(x[1])
}

fcstMat_regi <- function( connection , Phantom, org_level, query,  intermittent, DateMask, yrfreq, status) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, org_level, DateMask))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)

  returnobject <- fcstgetAccuracy(myts, intermittent, status)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}
