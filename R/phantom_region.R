f_ph_reg <- function(x, connection, ilevel, iYYYY, ifreq) {
  phantom <- x[1]
  iorg_level <- x[2]
  statusmessage <- 0
  statusmessage$status <- 'Initialized'
  statusmessage$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster($1,$2,$3)"
  fcaccuracy <- extTryCatch(fcstPhantom_reg(connection , phantom, iorg_level, iquery, FALSE, iYYYY, ifreq , statusmessage))
  print(x[1])
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, iorg_level, iYYYY)
  print(x[1])
}

fcstPhantom_reg <- function( connection , Phantom, org_level, query,  intermittent, DateMask, yrfreq, status) {


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
