f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  print(x)
  phantom <- x[3]
  iorg_level <- x[2]
  region <- x[1]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}

  print(region)
  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'


  iquery <- "select to_char(a,'YYYYMM') requested_delivery_date ,b.revenue
               from  generate_series(to_date('201501','YYYYMM')::date, to_date($1,'YYYY-MM-DD'),'1 month') a
                  join revenue_vol_bw_vol_p b on trim(b.bw_date) = trim(to_char(a,'YYYYMM'))
                where business_grp = $2 and sales_org =$3 and unit ='$' order by 1 asc "
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, region, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  # fcaccuracy <- extTryCatch(fcstMat_region(con , "phantom", "iorg_level", "region", iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, region, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, region, query,  intermittent, sma_only, DateMask, yrfreq, status, todate) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c( todate,region,org_level))#Phantom, org_level, DateMask, region,
  # df_postgres <- RPostgreSQL::dbGetQuery(con,  iquery, c( "2018-06-03","El","2000"))#Phantom, org_level, DateMask, region,
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


