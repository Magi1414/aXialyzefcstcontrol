f_mat_gl <- function(x, connection, ilevel, iorg_level, iYYYY, ifreq) {
  phantom <- x[1]
  statusmessage <- 0
  statusmessage$status <- 'Initialized'
  statusmessage$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterial($1,$2)"
  fcaccuracy <- extTryCatch(fcstmaterial(connection , phantom, iquery, FALSE, iYYYY, ifreq , statusmessage))
  print(x[1])
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, iorg_level, iYYYY)
  print(x[1])
}

fcstmaterial <- function( connection , Phantom, query,  intermittent, DateMask, yrfreq, status) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, DateMask))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)

  returnobject <- fcstgetAccuracy(myts, intermittent, status)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}
