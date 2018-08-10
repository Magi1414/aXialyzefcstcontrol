f_mat_cust <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie) {
  phantom <- x[1]
  iorg_level <- x[2]
  customer <- x[3]
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqtyvalue_per_date_inclmaterial_cluster_payer($1,$2,$3,$4)"
  fcaccuracy <- extTryCatch(fcstMat_cust(connection , phantom, iorg_level, customer, iquery, FALSE, iYYYY, ifreq , status_message))
  print(x[1])
  print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, iorg_level, iYYYY, fcperiod, sendfcserie)
 # print(x[1])
}

fcstMat_cust <- function( connection , Phantom, org_level, customer, query,  intermittent, DateMask, yrfreq, status) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, org_level, DateMask, customer))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)

  returnobject <- fcstgetAccuracy(myts, intermittent, status)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}
