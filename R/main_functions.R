require("uuid")
library("foreach")

extTryCatch <- function(expr) {
  warn <- err <- NULL
  value <- withCallingHandlers(
    tryCatch(expr, error=function(e) {
      err <<- e
      NULL
    }), warning=function(w) {
      warn <<- w
      invokeRestart("muffleWarning")
    })
  list(value=value, warning=warn, error=err)
}
write_fcdata_todbx <- function(position, connectionx, x, uuidx, fcperiodx){
  qry = "INSERT INTO public.forecast_r(
  date_created, forecastperiod, forecast_forperiodplus, forecast_uuid,
  fcvalue)
  VALUES (  now(), $1,$2,$3, $4)"
  dbs <- dbSendQuery(connectionx, qry, c(fcperiodx, position, uuidx,  x[position]))
  dbClearResult(dbs)
}

write_fcobject_todb <- function(connectionpga, fcaccuracy, ilevel, phantom,  org_level, iYYYY, fcperiod, sendseriedetails){
  qry = "insert into fcst_accuracy (uuid, fcst_accuracy_measurement,
                                   fcst_method, material,
                                   geography, MAPE, created_date,
                                   created_time,output_description, message, volume, time_mask, fcperiod)
                                   values ($1,$2,$3,$4,$5, $6,$7,$8, $9, $10, $11, $12, $13)"
  datex=format(as.Date(Sys.Date(),origin="1970-01-01"))
  datet=format(as.character(Sys.time()))

  if (is.null(fcaccuracy$error[1])) {

    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$arimauuid),"Null",fcaccuracy$value$arimauuid), ilevel,"auto.arima",phantom, org_level , ifelse(is.null(fcaccuracy$value$arima[6]),"Null",fcaccuracy$value$arima[6]),datex,datet,"Completed",ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]),ifelse(is.null(fcaccuracy$value$totalvolume),0, fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$stlfuuid),"Null",fcaccuracy$value$stlfuuid) , ilevel, "stlf", phantom, org_level , ifelse(is.null(fcaccuracy$value$stlf[6]),"Null",fcaccuracy$value$stlf[6]), datex,datet, "Completed", ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$hwuuid),"Null",fcaccuracy$value$hwuuid), ilevel, "hw", phantom, org_level , ifelse(is.null(fcaccuracy$value$hw[6]),"Null",fcaccuracy$value$hw[6]), datex,datet, "Completed", ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$arimaintuuid),"Null",fcaccuracy$value$arimaintuuid) ,ilevel, "arimaint", phantom, org_level , ifelse(is.null(fcaccuracy$value$arimaint[6]),"Null",fcaccuracy$value$arimaint[6]), datex,datet, "Completed", ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    }
  else
  {
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$arimauuid),"Null",fcaccuracy$value$arimauuid) ,ilevel,"auto.arima",phantom,org_level,ifelse(is.null(fcaccuracy$value$arima[6]),"Null",fcaccuracy$value$arima[6]),datex,datet,"Error",ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]),ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume),iYYYY,fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$stlfuuid),"Null",fcaccuracy$value$stlfuuid) ,ilevel, "stlf", phantom, org_level, ifelse(is.null(fcaccuracy$value$stlf[6]),"Null",fcaccuracy$value$stlf[6]), datex,datet, "Error", ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$hwuuid),"Null",fcaccuracy$value$hwuuid) , ilevel, "hw", phantom, org_level, ifelse(is.null(fcaccuracy$value$hw[6]),"Null",fcaccuracy$value$hw[6]), datex,datet, "Error", ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(ifelse(is.null(fcaccuracy$value$arimaintuuid),"Null",fcaccuracy$value$arimaintuuid) ,ilevel, "arimaint", phantom, org_level, ifelse(is.null(fcaccuracy$value$arimaint[6]),"Null",fcaccuracy$value$arimaint[6]), datex,datet, "Error", ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
  }
if(sendseriedetails ){
  fsarima <- fcaccuracy$value$arimafc$mean
  if(is.null(fsarima)==FALSE){
    print (fsarima)
       lapply(1:length(fsarima), write_fcdata_todbx, connectionpga, fsarima, fcaccuracy$value$arimauuid, fcperiod)
  }
  fss <- fcaccuracy$value$stlffc$mean
  if(is.null(fss)==FALSE){
    lapply(1:length(fss), write_fcdata_todbx, connectionpga, fss, fcaccuracy$value$stlfuuid, fcperiod)
  }
  fsh <- fcaccuracy$value$hwfc$mean
  if(is.null(fsh)==FALSE){
     lapply(1:length(fsh), write_fcdata_todbx, connectionpga, fsh, fcaccuracy$value$arimauuid, fcperiod)
  }
  fsi <- fcaccuracy$value$arimaintfc$mean
  if(is.null(fsi)==FALSE){
    lapply(1:length(fsi), write_fcdata_todbx, connectionpga, sfi, fcaccuracy$value$arimaintuuid, fcperiod)
  }
}
}



fcstgetAccuracy <- function(myts, intermittent, status){
  result <- 1
  result$stlfuuid <-  UUIDgenerate(use.time=NA)
  result$fcsthwuuid <-  UUIDgenerate(use.time=NA)
  result$arimauuid <-  UUIDgenerate(use.time=NA)
  result$arimaintuuid <-  UUIDgenerate(use.time=NA)
  if(intermittent == TRUE){
    fcsta <- tsintermittent::imapa(myts)
    fcstlf <- stlf(myts, lambda=BoxCox.lambda(myts))
    fcsthw <- forecast(HoltWinters(thets), h=20)
  }
  else{
    c <- extTryCatch(tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
          maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima"))
    if (is.null(c$error[1])) {thets <- c$value$yadj} else {thets <- myts}
    fcsta <- forecast::auto.arima(thets)
    fcstlf <- stlf(thets, lambda=BoxCox.lambda(thets))
    fcsthw <- forecast(HoltWinters(thets), h=20)
  }

  result$arima <- forecast::accuracy(fcsta)
  result$arimafc <- forecast(fcsta, h= 20)
  result$stlf <- forecast::accuracy(fcstlf)
  result$stlffc <- forecast(fcstlf, h= 20)
  result$fcsthw <- forecast::accuracy(fcsthw)
  result$fcsthwfc <- fcsthw

  status$status <- "Completed"
  status$message <- "Completed"

  return (result)
}


Clean_Phantom_Cluster <- function( connectionpg , Phantom, Cluster, DateMask, yrfreq) {

  query1 <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster($1,$2)"
  df_postgres <- RPostgreSQL::dbGetQuery(connectionpg,  query, c(Phantom, DateMask))
  df_postgres <- RPostgreSQL::dbGetQuery(connectionpg,  query)

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

}

Clean_Phantom_Cluster_Customer <- function( connectionpg , Phantom, Cluster, Customer, DateMask, yrfreq) {

  query1 <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster_division_custom($1,$2)"
  df_postgres <- RPostgreSQL::dbGetQuery(connectionpg,  query, c(Phantom, DateMask))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  print(myts)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

}


