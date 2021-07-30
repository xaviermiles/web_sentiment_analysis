library(tidyverse)
library(highcharter)

daily <- read_csv("GNH/output/gnh_nz_daily.csv")

# daily
hchart(daily, "line", hcaes(datetime_to_timestamp(date), gnh)) %>%
  hc_xAxis(
    labels = list(format = "{value:%b-%y}"),
    title = "date"
  ) %>%
  hc_yAxis(
    title = ""
  )

# weekly
daily %>%
  group_by(start_week = floor_date(date, "weeks")) %>%
  summarise(gnh = mean(gnh)) %>%
  hchart("line", hcaes(datetime_to_timestamp(start_week), gnh)) %>%
  hc_xAxis(
    labels = list(format = "{value:%b-%y}"),
    title = "date"
  ) %>%
  hc_yAxis(
    title = ""
  )
