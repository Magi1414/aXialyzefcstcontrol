
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

write_fcobject_todb <- function(connection, fcaccuracy, ilevel, phantom,  org_level, iYYYY){
  qry = "insert into fcst_accuracy (fcst_accuracy_measurement, material, geography, MAPE, created_date,created_time,
  output_description, message, volume, time_mask, mape_stlf, mape_hw, mape_arima_int) values ($1,$2,$3,$4,$5, $6,$7,$8, $9, $10, $11 ,$12, $13)"
  datex=format(as.Date(Sys.Date(),origin="1970-01-01"))
  datet=format(as.character(Sys.time()))

  if (is.null(fcaccuracy$error[1])) {
    dbSendQuery(connection, qry, c(ilevel, phantom, org_level , ifelse(is.null(fcaccuracy$value$arima[6]),"Null",fcaccuracy$value$arima[6]), datex,datet, "Completed", ifelse(is.null(fcaccuracy$warning[1]),"No errormessage",fcaccuracy$warning[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY,ifelse(is.null(fcaccuracy$value$stlf[6]),"Null",fcaccuracy$value$stlf[6]), ifelse(is.null(fcaccuracy$value$hw[6]),"Null",fcaccuracy$value$hw[6]), ifelse(is.null(fcaccuracy$value$arimaint[6]),"Null",fcaccuracy$value$arimaint[6])))
  }
  else
  {
    dbSendQuery(connection, qry, c(ilevel, phantom, org_level, ifelse(is.null(fcaccuracy$value$arima[6]),"Null",fcaccuracy$value$arima[6]), datex,datet, "Error", ifelse(is.null(fcaccuracy$error[1]),"No errormessage",fcaccuracy$error[1]), ifelse(is.null(fcaccuracy$value$totalvolume),0,fcaccuracy$value$totalvolume), iYYYY,ifelse(is.null(fcaccuracy$value$stlf[6]),"Null",fcaccuracy$value$stlf[6]), ifelse(is.null(fcaccuracy$value$hw[6]),"Null",fcaccuracy$value$hw[6]), ifelse(is.null(fcaccuracy$value$arimaint[6]),"Null",fcaccuracy$value$arimaint[6])))

  }
}

fcstgetAccuracy <- function(myts, intermittent, status){

  if(intermittent == TRUE){
    fcsta <- tsintermittent::imapa(myts)
    fcstlf <- stlf(myts, lambda=BoxCox.lambda(myts))
  }
  else{
    c <- extTryCatch(tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
          maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima"))
    if (is.null(c$error[1])) {thets <- c$value$yadj} else {thets <- myts}
    fcsta <- forecast::auto.arima(thets)
    fcstlf <- stlf(thets, lambda=BoxCox.lambda(thets))
  }
  result <- 1
  result$arima <- forecast::accuracy(fcsta)
  result$stlf <- forecast::accuracy(fcstlf)
  status$status <- "Completed"
  status$message <- "Completed"
  return (result)
}


Clean_Phantom_Cluster <- function( connection , Phantom, Cluster, DateMask, yrfreq) {

  query1 <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster($1,$2)"
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, DateMask))
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query)

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

}

Clean_Phantom_Cluster_Customer <- function( connection , Phantom, Cluster, Customer, DateMask, yrfreq) {

  query1 <- "select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster_division_custom($1,$2)"
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c(Phantom, DateMask))

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  print(myts)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

}


