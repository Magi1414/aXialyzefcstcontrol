require("uuid")
library("foreach")
require("forecast")

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
  VALUES (  now(), $1,$2,$3, case when $4 ilike '%NA%' then null else $4 end::double precision)"
  dbs <-  extTryCatch( dbSendQuery(connectionx, qry, c(fcperiodx, position, uuidx, x[position] )))
  if (is.null(dbs$value)==FALSE){dbClearResult(dbs$value)}
  #no need to print -->if (is.null(fcsthwcc$error[1])==FALSE){print }
}

write_fcobject_todb <- function(connectionpga, fcaccuracy, ilevel, phantom,  org_level, iYYYY, fcperiod, sendseriedetails, fcrun){
  qry = "insert into fcst_accuracy (fcrun, uuid, fcst_accuracy_measurement,
                                   fcst_method, material,
                                   geography, MAPE, mase, mape_limited, mase_limited, theilu_limited, fca_limited, created_date,
                                   created_time,output_description, message, volume, time_mask, fcperiod)
                                   values ($1,$2,$3,$4,$5, $6,$7,$8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"
  datex=format(as.Date(Sys.Date(),origin="1970-01-01"))
  datet=format(as.character(Sys.time()))

  if (is.null(fcaccuracy$error[1])) {
    errorstatus <- "completed"} else {errorstatus <- fcaccuracy$error[1]}

    dbs <- dbSendQuery(connectionpga, qry, c(fcrun, ifelse(is.null(fcaccuracy$value$arimauuid),"Null",fcaccuracy$value$arimauuid), ilevel,ifelse(is.null(fcaccuracy$value$arimaname),"Null",fcaccuracy$value$arimaname),phantom, org_level ,
                                             ifelse(is.null(fcaccuracy$value$arima[5]),"Null",fcaccuracy$value$arima[5]),ifelse(is.null(fcaccuracy$value$arima[6]),"Null",fcaccuracy$value$arima[6]),
                                             ifelse(is.null(fcaccuracy$value$arimalimited[[5]]),"Null",fcaccuracy$value$arimalimited[[5]]),ifelse(is.null(fcaccuracy$value$arimalimited$MASE),"Null",fcaccuracy$value$arimalimited$MASE),
                                             ifelse(is.null(fcaccuracy$value$arimalimited[[7]]),"Null",fcaccuracy$value$arimalimited[[7]]),
                                             ifelse(is.null(fcaccuracy$value$arimalimited$FCA),"Null",fcaccuracy$value$arimalimited$FCA), datex,datet,errorstatus,  ifelse(is.null(fcaccuracy$value$arimaerror), "Null", fcaccuracy$value$arimaerror),ifelse(is.null(fcaccuracy$value$totalvolume),0, fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(fcrun, ifelse(is.null(fcaccuracy$value$stlfuuid),"Null",fcaccuracy$value$stlfuuid) , ilevel, ifelse(is.null(fcaccuracy$value$stlfname),"Null",fcaccuracy$value$stlfname), phantom, org_level ,
                                             ifelse(is.null(fcaccuracy$value$stlf[5]),"Null",fcaccuracy$value$stlf[5]), ifelse(is.null(fcaccuracy$value$stlf[6]),"Null",fcaccuracy$value$stlf[6]),
                                             ifelse(is.null(fcaccuracy$value$stlflimited[[5]]),"Null",fcaccuracy$value$stlflimited[[5]]), ifelse(is.null(fcaccuracy$value$stlflimited$MASE),"Null",fcaccuracy$value$stlflimited$MASE),
                                             ifelse(is.null(fcaccuracy$value$stlflimited[[7]]),"Null",fcaccuracy$value$stlflimited[[7]]),
                                             ifelse(is.null(fcaccuracy$value$stlflimited$FCA),"Null",fcaccuracy$value$stlflimited$FCA), datex,datet, errorstatus, ifelse(is.null(fcaccuracy$value$stlferror), "Null", fcaccuracy$value$stlferror), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(fcrun, ifelse(is.null(fcaccuracy$value$hwuuid),"Null",fcaccuracy$value$hwuuid), ilevel, ifelse(is.null(fcaccuracy$value$hwname),"Null",fcaccuracy$value$hwname), phantom, org_level ,
                                             ifelse(is.null(fcaccuracy$value$hw[5]),"Null",fcaccuracy$value$hw[5]), ifelse(is.null(fcaccuracy$value$hw[6]),"Null",fcaccuracy$value$hw[6]),
                                             ifelse(is.null(fcaccuracy$value$hwlimited[[5]]),"Null",fcaccuracy$value$hwlimited[[5]]), ifelse(is.null(fcaccuracy$value$hwlimited$MASE),"Null",fcaccuracy$value$hwlimited$MASE),
                                             ifelse(is.null(fcaccuracy$value$hwlimited[[7]]),"Null",fcaccuracy$value$hwlimited[[7]]),
                                             ifelse(is.null(fcaccuracy$value$hwlimited$FCA),"Null",fcaccuracy$value$hwlimited$FCA), datex,datet, errorstatus, ifelse(is.null(fcaccuracy$value$hwerror), "Null", fcaccuracy$value$hwerror), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)
    dbs <- dbSendQuery(connectionpga, qry, c(fcrun, ifelse(is.null(fcaccuracy$value$arimaintuuid),"Null",fcaccuracy$value$arimaintuuid) ,ilevel, ifelse(is.null(fcaccuracy$value$arimaintname),"Null",fcaccuracy$value$arimaintname), phantom, org_level ,
                                             ifelse(is.null(fcaccuracy$value$arimaint[5]),"Null",fcaccuracy$value$arimaint[5]), ifelse(is.null(fcaccuracy$value$arimaint[6]),"Null",fcaccuracy$value$arimaint[6]),
                                             ifelse(is.null(fcaccuracy$value$arimaintlimited[[5]]),"Null",fcaccuracy$value$arimaintlimited[[5]]), ifelse(is.null(fcaccuracy$value$arimaintlimited$MASE),"Null",fcaccuracy$value$arimaintlimited$MASE),
                                             ifelse(is.null(fcaccuracy$value$arimaintlimited[[7]]),"Null",fcaccuracy$value$arimaintlimited[[7]]),
                                             ifelse(is.null(fcaccuracy$value$arimaintlimited$FCA),"Null",fcaccuracy$value$arimaintlimited$FCA),datex,datet, errorstatus, ifelse(is.null(fcaccuracy$value$arimainterror), "Null", fcaccuracy$value$arimainterror), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY, fcperiod))
    dbClearResult(dbs)

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
     lapply(1:length(fsh), write_fcdata_todbx, connectionpga, fsh, fcaccuracy$value$hwuuid, fcperiod)
  }
  fsi <- fcaccuracy$value$arimaintfc$mean
  if(is.null(fsi)==FALSE){
    lapply(1:length(fsi), write_fcdata_todbx, connectionpga, fsi, fcaccuracy$value$arimaintuuid, fcperiod)
  }
}
}

computeMASE <- function(forecast,train,test,period){

  # forecast - forecasted values
  # train - data used for forecasting .. used to find scaling factor
  # test - actual data used for finding MASE.. same length as forecast
  # period - in case of seasonal data.. if not, use 1

  forecast <- as.vector(forecast)
  train <- as.vector(train)
  test <- as.vector(test)

  n <- length(train)
  scalingFactor <- sum(abs(train[(period+1):n] - train[1:(n-period)])) / (n-period)

  et <- abs(test-forecast)
  qt <- et/scalingFactor
  meanMASE <- mean(qt)
  return(meanMASE)
}

computeFCA <- function(forecast,test){

  # forecast - forecasted values
  # test - actual data used for finding FCA.. same length as forecast

  forecast <- as.vector(forecast)
  test <- as.vector(test)

  et <- ifelse(test == 0,ifelse(forecast == 0 , 0,1) , abs(test-forecast)/abs(test))*pmax(test,1)
  FCerror <- sum(et)/sum(pmax(test,1))
  return(max(0,1-FCerror))
}

limited_accuracy <- function(myts,fc, pfrequency, testperiods){
  tstimes <- getTStime(myts, pfrequency)

  observations <- window(myts,start=c(tstimes[[length(tstimes)-testperiods]][1],tstimes[[length(tstimes)-testperiods]][2]))
  #trainingdata <- window(myts,end=c(tstimes[[length(tstimes)-testperiods-1]][1],tstimes[[length(tstimes)-testperiods-1]][2]))
  #fcobject <- forecast(trainingdata, h=testperiods, model=fc)
  fitted <- window(fitted(fc),start=c(tstimes[[length(tstimes)-testperiods]][1],tstimes[[length(tstimes)-testperiods]][2]))
  ac <- accuracy(fitted, observations)
  ac$MASE <- computeMASE(fitted, myts, observations, 1)
  ac$FCA <- computeFCA(fitted, observations)
  return(ac)
}

fcstgetAccuracy <- function(myts, intermittent, status, thefrequency){
  accuracyperiods <- 3
  result <- 1
  result$stlfuuid <-  UUIDgenerate(use.time=NA)
  result$hwuuid <-  UUIDgenerate(use.time=NA)
  result$arimauuid <-  UUIDgenerate(use.time=NA)
  result$arimaintuuid <-  UUIDgenerate(use.time=NA)
  result$arimaintname <- "arima int"
  result$arimaname <- "auto.arima"
  result$stlfname <- "stlf"
  result$hwname <- "hw"

  if(intermittent == TRUE){
    fcstatc <- extTryCatch(forecast::auto.arima(myts))
    fcstainttc <- extTryCatch(tsintermittent::imapa(myts))
    result$arimaintname <- "imapa"
    fcstlftc <- extTryCatch(stlf(myts, lambda=BoxCox.lambda(myts)))
    fcsthwcc <- extTryCatch(HoltWinters(myts))
    if (is.null(fcsthwcc$error[1])){
      fcsthwc <- fcsthwcc} else {
        result$hwname <- "ets AAN"
        fcsthwc <- ets(y=myts,model="AAN")
      }
  }
  else{
    c <- extTryCatch(tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
          maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima"))
    cint <- extTryCatch(tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
          maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
          args.tsmethod = list(lambda=0)))
    if (is.null(c$error[1])) {thets <- c$value$yadj} else {thets <- myts}
    if (is.null(cint$error[1])) {thetsint <- cint$value$yadj} else {thetsint <- myts}
    fcstatc <- extTryCatch(forecast::auto.arima(thets))
    fcstainttc <- extTryCatch(forecast::auto.arima(thetsint, lambda = 0))
    fcstlftc <- extTryCatch(stlf(thets, lambda=BoxCox.lambda(thets)))
    fcsthwcc <- extTryCatch(HoltWinters(thets))
    if (is.null(fcsthwcc$error[1])){
      fcsthwc <- fcsthwcc} else {
        result$hwname <- "ets AAN"
        fcsthwc <- extTryCatch(ets(y=thets,model="AAN"))
      }
  }

    if(is.null(fcstatc$error[1])){
        fcsta <- fcstatc$value
        result$arimafc <- forecast(fcsta, h= 20)
        result$arima <- accuracy(fcsta)
        result$arimalimited <- limited_accuracy(myts, fcsta, thefrequency, accuracyperiods )
        result$arimaerror <- "succes"}
    else {result$arimaerror <- fcstatc$error[1]}
    if(is.null(fcstainttc$error[1])){
      fcstaint <- fcstainttc$value
      result$arimaint <- forecast::accuracy(fcstaint)
      result$arimaintlimited <- limited_accuracy(myts, fcstaint, thefrequency, accuracyperiods )
      result$arimaintfc <- forecast(fcstaint, h= 20)
      result$arimainterror <- "succes"}
    else {result$arimainterror <- fcstainttc$error[1]}
    if(is.null(fcstlftc$error[1])){
      fcstlf <- fcstlftc$value
      result$stlf <- forecast::accuracy(fcstlf)
      result$stlflimited <- limited_accuracy(myts, fcstlf, thefrequency, accuracyperiods )
      result$stlffc <- forecast(fcstlf, h= 20)
      result$stlferror <- "succes"}
    else {result$stlferror <- fcstlftc$error[1]}
  if(is.null(fcsthwc$error[1])){
    fcsthwv <- fcsthwc$value
    #result$hwfv <- fcsthwv
    result$hwfc <- forecast(fcsthwv, h=20)
    result$hw <- forecast::accuracy(result$hwfc)
    result$hwlimited <- limited_accuracy(myts, fcsthwv, thefrequency, accuracyperiods )
    result$hwerror <- "succes"}
    else {result$hwerror <- fcsthwc$error[1]}

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

getTStime <- function(ats, frequency){
  start <- start(ats)
  end <- end(ats)
  time <- list()
  time[[1]] <- start
  m <- 2
  while(!(identical(start, end))){
    start[2] <- start[2] + 1
    if (start[2]==(frequency + 1)){
      start[1] <- start[1] + 1
      start[2] <- 1
    }
    time[[m]] <- start
    m <- m + 1
  }
  return(time)
}


