library("ggplot2")
library("RPostgreSQL")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nagios")

hosts <- dbReadTable(con,"hosts")
ggplot(hosts, aes(nagios_hosts, fill=factor(quartile))) + geom_histogram(binwidth=10) + xlab("Host count") + ylab("Population") + ggtitle("Nagios samples")

w_i <- dbReadTable(con, "weekly")
qplot(normalized_incidents, data = w_i, geom="histogram", binwidth=5, xlim=c(0, 1000), xlab="Nagios alert per host", ylab="count per week") + facet_grid(quartile ~ .)

h_i <- dbReadTable(con, "worst_hour")
ggplot(h_i, aes(hour_of_day, hourly, group = hour_of_day)) + geom_boxplot() + xlab("Hour of Day (UTC)") + ylab("Alerts per hour")
ggplot(h_i, aes(hour_of_day, hourly, group = hour_of_day)) + facet_grid(quartile ~ .) + geom_boxplot() + xlab("Hour of Day (UTC)") + ylab("Alerts per hour")

dow_i <- dbReadTable(con, "worst_day_of_week")
ggplot(dow_i, aes(day_of_week, daily)) + facet_grid(quartile ~ .) + geom_area() + scale_x_discrete(breaks=c(0,1,2,3,4,5,6), labels=c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")) + xlab("Day of week") + ylab("Alerts per hour") + ggtitle("Notifying Alerts per Day")

noisy_hosts <- dbReadTable(con, "noisiest_hosts")
ggplot(noisy_hosts, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (overall)")

noisy_outlier <- dbReadTable(con, "noisiest_hosts_outlier")
ggplot(noisy_outlier, aes(rnk, hourly)) + geom_point() + geom_line() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (outlier)")

noisy_no_outlier <- dbReadTable(con, "noisiest_hosts_no_outlier")
ggplot(noisy_no_outlier, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Hosts ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest hosts (without outlier)")

noisy_checks <- dbReadTable(con, "noisiest_checks")
ggplot(noisy_checks, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Checks ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest checks (overall)")

checks_outlier <- dbReadTable(con, "noisiest_checks_outlier")
ggplot(checks_outlier, aes(rnk, hourly)) + geom_point() + geom_line() + xlab("Checks ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest checks (outlier)")

checks_no_outlier <- dbReadTable(con, "noisiest_checks_no_outlier")
ggplot(checks_no_outlier, aes(rnk, hourly)) + facet_grid(quartile ~ .) + geom_area() + xlab("Checks ranked by noise") + ylab("Alerts per hour") + ggtitle("Noisiest checks (without outlier)")

survival <- dbReadTable(con, "survival_occurrence")
ggplot(survival, aes(age_days, days_occurring, color=factor(quartile))) + geom_point() + scale_colour_brewer(palette="Set1") + xlab("Age between earliest and latest occurrence") + ylab("Number of days occurring") + ggtitle("Alert age & frequency of occurrence")
