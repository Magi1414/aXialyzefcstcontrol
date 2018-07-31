f_ph_gl <- function(x, connection, ilevel, iorg_level, iYYYY, ifreq) {
  phantom <- x[1]
  statusmessage <- 0
  statusmessage$status <- 'Initialized'
  statusmessage$message <- 'Initialized'
  iquery <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost($1,$2)"
  fcaccuracy <- extTryCatch(fcstPhantom(connection , phantom, iquery, FALSE, iYYYY, ifreq , statusmessage))
  print(x[1])
  qry = "insert into fcst_accuracy (fcst_accuracy_measurement, material, geography, MAPE, created_date,created_time,
  output_description, message, volume, time_mask) values ($1,$2,$3,$4,$5, $6,$7,$8, $9, $10)"
  datex=format(as.Date(Sys.Date(),origin="1970-01-01"))
  datet=format(as.character(Sys.time()))
  print (statusmessage$status)
  if (is.null(fcaccuracy$error[1])) {
    dbSendQuery(connection, qry, c(ilevel, phantom, iorg_level, ifelse(is.null(fcaccuracy$value[6]),"Null",fcaccuracy$value[6]), datex,datet, "Completed", ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume)), iYYYY)
  }
  else
  {
    dbSendQuery(connection, qry, c(ilevel, phantom, iorg_level, ifelse(is.null(fcaccuracy$value[6]),"Null",fcaccuracy$value[6]), datex,datet, "Error", ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume)), iYYYY)

  }
  print(x[1])
}

fcstPhantom <- function( connection , Phantom, query,  intermittent, DateMask, yrfreq, status) {


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
