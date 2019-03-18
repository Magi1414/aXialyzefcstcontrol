f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, sendfcserie, fcrun) {
  print(x)
  phantom <- x[2]
  continent <- x[1]
  business_grp <- x[8]
  scenario <- x[9]
  todate <- x[6]
  fcperiod <- x[7]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}



  print(continent)
  print(phantom)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'


  iquery <- "select to_char(a,'YYYYQ') requested_delivery_date ,sum(b.revenue) revenue
                from  generate_series(to_date('201501','YYYYMM')::date, to_date($1,'YYYYMM'),'1 month') a
                left join revenue_vol_selection_continent_bis b on b.business_grp = $2 and b.bw_date = trim(to_char(a,'YYYYMM'))
                where  scenario = 'global'
                   group by to_char(a,'YYYYQ')
                order by 1 asc"

  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom,  continent, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate,business_grp,scenario))


  #fcaccuracy <- extTryCatch(fcstMat_region(con , phantom,  continent, iquery, FALSE,sma_only, "YYYY-MM", 12 , status_message, todate,business_grp,scenario))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, continent, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom,  continent, iquery,  intermittent, sma_only, DateMask, yrfreq, status, todate,business_grp,scenario) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  iquery, c( todate, business_grp))
  # df_postgres <- RPostgreSQL::dbGetQuery(con,  iquery, c( "2018-06-30","Americas,"5000 - El - global - $","El","global"))#Phantom, org_level, DateMask, region,
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

