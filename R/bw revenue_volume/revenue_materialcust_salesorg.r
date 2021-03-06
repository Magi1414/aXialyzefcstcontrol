f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, sendfcserie, fcrun) {
  print(x)
  phantom <- x[2]
  org_level <- x[1]

  todate <- x[6]
  fcperiod <- x[7]
  business_grp <- x[8]
  sales_org <- x[9]
  material<- x[10]
  customer<- x[11]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}

  # x <- df
  # phantom <- x[1,2]
  # org_level <- x[1,1]
  # business_grp <- x[1,8]
  # sales_org <- x[1,9]
  # todate <- x[1,6]
  # fcperiod <- x[1,7]
  # material <- x[1,10]
  # customer <- x[1,11]


  print(phantom)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'


  iquery <- "select to_char(a,'YYYYMM') requested_delivery_date ,b.revenue
  from  generate_series(to_date('201301','YYYYMM')::date, to_date($1,'YYYY-MM'),'1 month') a
  join revenue_vol_matcust_bw_selection_m b on b.bw_date = trim(to_char(a,'YYYYMM'))
  where sales_org = $2 and material = $3 and business_grp = $4 and customer = $5
  order by 1"
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, org_level,  iquery, FALSE,sma_only, iYYYY, ifreq ,
                                            status_message, todate,business_grp,sales_org,material,customer))


  #fcaccuracy <- extTryCatch(fcstMat_region(con , phantom,  org_level, iquery, FALSE,sma_only, "YYYY-MM", 12 , status_message,todate,business_grp,sales_org,material,customer))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom,org_level,  iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, query,  intermittent, sma_only, DateMask, yrfreq, status, todate,business_grp,sales_org,material,customer) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c( todate,sales_org,material,business_grp,customer))
  # df_postgres <- RPostgreSQL::dbGetQuery(con,  iquery, c( todate,sales_org,material,business_grp,customer))
  print (df_postgres)
  myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = yrfreq)
  # myts <- ts(df_postgres[ ,2], start = c(2015, 1), frequency = ifreq)
  ##return (myts)

  returnobject <- fcstgetAccuracy(myts, intermittent, status, yrfreq,sma_only)
  returnobject$totalvolume = sum(myts)
  returnobject$ts <- myts
  status$status <- "Completed"
  status$message <- "Completed"

  return (returnobject)

}


