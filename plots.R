library("RPostgreSQL")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nagios")
rs <- dbSendQuery(con,"select * from hosts order by host_count desc")
hosts <- fetch(rs, n=-1)
rs <- dbSendQuery(con, "select * from weekly_incidents")
w_i <- fetch(rs, ns=-1)