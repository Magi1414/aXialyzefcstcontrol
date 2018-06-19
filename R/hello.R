# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

hello <- function() {
  print("Hello, world!")
}

require("RPostgreSQL")
require(forecast)
require("tsoutliers")
require(ggplot2)
require("tsintermittent")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "Atotech",
                 host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 5432,
                 user = "aXialyze", password = "aXialyze0000")
df <- dbGetQuery(con, "SELECT materialghost, kilo, periods, order_periods, continuity, time_serie_category,
                          order_value FROM public.time_serie_categories_ghosts where time_serie_category = 'Continuous'" )

 f <- function(x, connection) {
 phantom <- x[1]
 fcaccuracy <- fcstPhantom(connection , Phantom, FALSE, 'YYYY-IW', 52 )

 qry = "insert into fcst_accuracy (fcst_accuracy_measurement, material, geography, MAPE, created_date,
            output_description) values ($1,$2,$3,$4,$5, $6)"
 dbSendQuery(connection, qry, c("phantom_overall_Continous", phantom, "Global", fcaccuracy$MAPE, now(), fcaccuracy))
}

 ##apply(df, 1, f, connection = con)
