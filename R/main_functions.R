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

Clean_Phantom_Cluster <- function( connection , Phantom, Cluster, DateMask, yrfreq) {

  query1 <- paste("select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster(", Phantom, "")
  query2 <- paste(query1, ",", "")
  query2 <- paste(query2, Cluster, "")
  query2 <- paste(query2, ",", "")
  query3 <- paste(query2, DateMask, "")
  query <- paste(query3 , ")","")
  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query)

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

}

Clean_Phantom_Cluster_Customer <- function( connection , Phantom, Cluster, Customer, DateMask, yrfreq) {

  query1 <- paste("select requested_deliv_date,liters from get_orderqty_per_date_inclmaterialghost_cluster_division_custom(", Phantom, "")
  query2 <- paste(query1, ",", "")
  query2 <- paste(query2, Cluster, "")
  query2 <- paste(query2, ",", "")
  query2 <- paste(query2, DateMask, "")
  query2 <- paste(query2, ",", "")
  query3 <- paste(query2, Customer, "")
  query <- paste(query3 , ")","")
  print(query)

  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query)

  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  print(myts)
  c <- tsoutliers::tso( y = myts, types = c("AO",  "TC", "SLS"),
                        maxit = 1, discard.method = "en-masse", tsmethod = "auto.arima",
                        args.tsmethod = list(allowdrift = TRUE, ic = "bic"))
  c$myts <- myts
  return (c)

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
