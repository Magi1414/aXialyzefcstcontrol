f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  print(x)
  phantom <- x[2]
  iorg_level <- x[2]
  continent <- x[1]
  business_grp <- x[8]
  scenario <- x[9]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}

  print(continent)
  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'


  iquery <- "select to_char(a,'YYYYMM') requested_delivery_date ,b.revenue
                from  generate_series(to_date('201501','YYYYMM')::date, to_date('$1,'YYYY-MM-DD'),'1 month') a
                  join revenue_vol_selection_continent b on b.bw_date = trim(to_char(a,'YYYYMM'))
                    where continent = $2 and sales_org =left($3,4) and unit =right($3,1)
                      and business_grp = $4 and scenario = $5

              order by 1"
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, contient, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate,business_grp,scenario))
  # fcaccuracy <- extTryCatch(fcstMat_region(con , "phantom", "iorg_level", "region", iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, continent, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, continent, query,  intermittent, sma_only, DateMask, yrfreq, status, todate,business_grp,scenario) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c( todate,continent,Phantom,business_grp,scenario))
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


