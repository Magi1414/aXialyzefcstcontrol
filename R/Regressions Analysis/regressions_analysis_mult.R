require("RPostgreSQL")
require("tidyverse")


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "Atotech",
                 host = "axialyzeproduction.c5drkcatbgmm.eu-central-1.rds.amazonaws.com", port = 8080,
                 user = "aXialyze", password = "aXialyze0000")
df1 <- dbGetQuery(con, "SELECT
                  x1.sales::double precision as sales,
	                x1.index_v ::double precision as smartphones_pcb,
                  x2.index_v::double precision as smartphones_fec,
                  x3.index_v::double precision as smartphones_sc,
                  x4.index_v::double precision as computer_tablette_pcb,
                  x5.index_v::double precision as computer_tablette_fec,
                  x6.index_v::double precision as computer_tablette_sc,
                  x7.index_v::double precision as consumer_pcb,
                  x8.index_v::double precision as consumer_fec,
                  x9.index_v::double precision as consumer_sc,
                  x10.index_v::double precision as server_pcb,
                  x11.index_v::double precision as server_fec,
                  x12.index_v::double precision as server_sc,
                  x13.index_v::double precision as others_pcb,
                  x14.index_v::double precision as others_fec,
                  x15.index_v::double precision as others_sc
                  FROM revenue_sales_quarter_x1 x1
                  left join revenue_sales_quarter_x1 x2 on x1.bw_date = x2.bw_date
                  left join revenue_sales_quarter_x1 x3 on x1.bw_date = x3.bw_date
                  left join revenue_sales_quarter_x1 x4 on x1.bw_date = x4.bw_date
                  left join revenue_sales_quarter_x1 x5 on x1.bw_date = x5.bw_date
                  left join revenue_sales_quarter_x1 x6 on x1.bw_date = x6.bw_date
                  left join revenue_sales_quarter_x1 x7 on x1.bw_date = x7.bw_date
                  left join revenue_sales_quarter_x1 x8 on x1.bw_date = x8.bw_date
                  left join revenue_sales_quarter_x1 x9 on x1.bw_date = x9.bw_date
                  left join revenue_sales_quarter_x1 x10 on x1.bw_date = x10.bw_date
                  left join revenue_sales_quarter_x1 x11 on x1.bw_date = x11.bw_date
                  left join revenue_sales_quarter_x1 x12 on x1.bw_date = x12.bw_date
                  left join revenue_sales_quarter_x1 x13 on x1.bw_date = x13.bw_date
                  left join revenue_sales_quarter_x1 x14 on x1.bw_date = x14.bw_date
                  left join revenue_sales_quarter_x1 x15 on x1.bw_date = x15.bw_date


                  where
                  x1.end_market ='Smartphones' and x1.column1='PCB' and
                  x2.end_market ='Smartphones' and x2.column1='FEC' and
                  x3.end_market ='Smartphones' and x3.column1='SC' and
                  x4.end_market ='Computer & Tablet' and x4.column1='PCB' and
                  x5.end_market ='Computer & Tablet' and x5.column1='FEC' and
                  x6.end_market ='Computer & Tablet' and x6.column1='SC' and
                  x7.end_market ='Consumer (TV)' and x7.column1='PCB' and
                  x8.end_market ='Consumer (TV)' and x8.column1='FEC' and
                  x9.end_market ='Consumer (TV)' and x9.column1='SC' and
                  x10.end_market ='Server' and x10.column1='PCB' and
                  x11.end_market ='Server' and x11.column1='FEC' and
                  x12.end_market ='Server' and x12.column1='SC' and
                  x13.end_market ilike 'OTHER%' and x13.column1='PCB' and
                  x14.end_market ilike 'OTHER%' and x14.column1='FEC' and
                  x15.end_market ilike 'OTHER%' and x15.column1='SC'

                 " )



model1 <- lm(sales ~ smartphones_pcb + smartphones_fec + smartphones_sc
                      +computer_tablette_pcb + computer_tablette_fec + computer_tablette_sc +
             computer_tablette_pcb+ computer_tablette_fec + computer_tablette_sc +
               server_pcb + server_fec + server_sc +
               others_pcb + others_fec + others_sc , data = df1)
summary(model1)


plot(df1, main="",ylab="Sales [$]",
     xlab="Indicateur")


abline(model1)






