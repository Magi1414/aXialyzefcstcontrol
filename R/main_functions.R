fcstPhantom <- function( connection , Phantom, intermittent, DateMask, yrfreq) {

  query1 <- paste("select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost(", Phantom, "")
  query2 <- paste(query1, ",", "")
  query3 <- paste(query2, DateMask, "")
  query <- paste(query3 , ")","")
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query)

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  ##return (myts)
  return (fcstgetAccuracy(myts, intermittent))

}

fcstgetAccuracy <- function(myts, intermittent){
  if(intermittent == TRUE){
    fcst <- tsintermittent::imapa(myts)
  }
  else{
    c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
              maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
              args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
    fcst <- forecast::auto.arima(c$yadj)
  }
  result <- forecast::accuracy(fcst)
  return (result)
}
