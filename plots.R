library("ggplot2")
library("RPostgreSQL")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nagios")

rs <- dbSendQuery(con,"select * from hosts order by host_count desc")
hosts <- fetch(rs, n=-1)

w_i <- dbReadTable(con, "weekly")
qplot(normalized_incidents, data = w_i, geom="histogram", binwidth=5, xlim=c(0, 1000), xlab="Nagios alert per host", ylab="count per week") + facet_grid(quartile ~ .)

rs <- dbSendQuery(con, "select * from worst_hour")
h_i <- fetch(rs, ns=-1)
ggplot(h_i, aes(hour_of_day, hourly, group = hour_of_day)) + geom_boxplot() + xlab("Hour of Day (UTC)") + ylab("Alerts per hour")
ggplot(h_i, aes(hour_of_day, hourly, group = hour_of_day)) + facet_grid(quartile ~ .) + geom_boxplot() + xlab("Hour of Day (UTC)") + ylab("Alerts per hour")

dow_i <- dbReadTable(con, "worst_day_of_week")
ggplot(dow_i, aes(day_of_week, daily)) + facet_grid(quartile ~ .) + geom_area() + scale_x_discrete(breaks=c(0,1,2,3,4,5,6), labels=c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")) + xlab("Day of week") + ylab("Alerts per hour") + ggtitle("Notifying Alerts per Day")

noisy_hosts <- dbReadTable(con, "noisiest_hosts")
ggplot(noisy_hosts, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (overall)")

rs <- dbSendQuery(con, "select * from noisiest_hosts_outlier")
noisy_outlier <- fetch(rs, ns=-1)
ggplot(noisy_outlier, aes(rnk, hourly)) + geom_point() + geom_line() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (outlier)")

rs <- dbSendQuery(con, "select * from noisiest_hosts_no_outlier")
noisy_no_outlier <- fetch(rs, ns=-1)
ggplot(noisy_no_outlier, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (without outlier)")

rs <- dbSendQuery(con, "select * from noisiest_checks")
noisy_checks <- fetch(rs, ns=-1)
ggplot(noisy_checks, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Checks ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest checks (overall)")

rs <- dbSendQuery(con, "select * from noisiest_checks_outlier")
checks_outlier <- fetch(rs, ns=-1)
ggplot(checks_outlier, aes(rnk, hourly)) + geom_point() + geom_line() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (outlier)")

checks_no_outlier <- dbReadTable(con, "noisiest_checks_no_outlier")
ggplot(checks_no_outlier, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (without outlier)")

survival <- dbReadTable(con, "survival_occurrence")
ggplot(survival, aes(age_days, days_occurring, color=factor(quartile))) + geom_point() + scale_colour_brewer(palette="Set1") + xlab("Age between earliest and latest occurrence") + ylab("Number of days occurring") + ggtitle("Alert age & frequency of occurrence")
