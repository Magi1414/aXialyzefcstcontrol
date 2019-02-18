f_mat_regi <- function(x, connection, ilevel, iYYYY, ifreq, fcperiod, sendfcserie, fcrun, todate) {
  print(x)
  phantom <- x[2]
  iorg_level <- 'topdown'
  region <- x[1]
  #region <- 'China'
  # prod_grp <- 'El'
  prod_grp <- x[2]
  sma_only = FALSE
  if (x[3] == 1 ) {sma_only <- TRUE} else {sma_only <- FALSE}
  print(region)
  print(phantom)
  print (iorg_level)
  status_message <- 0
  status_message$status <- 'Initialized'
  status_message$message <- 'Initialized'


  iquery <- "SELECT to_date(invoice_date,'YYYYMM') requested_deliv_date_to, invoice_sum litre
                from revenue_p_month_filtred_prodgrp where invoice_date  <=  to_char(to_date($1,'YYYY-MM-DD'),'YYYYMM') and cluster = $2 and prod_grp = $3 order by 1 asc "
  fcaccuracy <- extTryCatch(fcstMat_region(connection , phantom, iorg_level, region, iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate,prod_grp))
  # fcaccuracy <- extTryCatch(fcstMat_region(con , "phantom", "iorg_level", "region", iquery, FALSE,sma_only, iYYYY, ifreq , status_message, todate))
  #print(x[1])
  #print(fcaccuracy)
  write_fcobject_todb(connection, fcaccuracy, ilevel, phantom, region, iYYYY, fcperiod, sendfcserie, fcrun,sma_only)
  # print(x[1])
}

fcstMat_region <- function( connection , Phantom, org_level, region, query,  intermittent, sma_only, DateMask, yrfreq, status, todate,prod_grp) {


  df_postgres <- RPostgreSQL::dbGetQuery(connection,  query, c( todate,region,prod_grp))#Phantom, org_level, DateMask, region,
  # df_postgres <- RPostgreSQL::dbGetQuery(con,  iquery, c( todate,region,prod_grp))
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


