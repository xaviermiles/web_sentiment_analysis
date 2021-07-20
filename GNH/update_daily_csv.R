# Updates daily GNH CSV

library("stringr")

source("GNH/get_daily.R")


CSV_FPATH <- "GNH/output/gnh_nz_daily.csv"
FIRST_DATE <- ymd("2019-05-12")

if (file.exists(CSV_FPATH)) {
  previous <- read_csv(CSV_FPATH, col_types = cols())
  start <- max(previous[["date"]]) + days(1)
} else {
  previous <- NULL
  start <- FIRST_DATE
}

if (start == today(tzone = "NZ")) {
  print("Finished: CSV was already up-to-date.")
} else {
  end <- today(tzone = "NZ") - days(1)
  new <- get_daily_gnh(start_date = start, end_date = end) %>%
    rename(date = tweet_at) %>%
    select(date, gnh)  # only need these columns for the portal

  if (!is.null(previous)) {
    merged <- rbind(previous, new)
  } else {
    merged <- new  # there was no previous data
  }

  expected_dates <- seq.Date(from = FIRST_DATE, to = end, by = "days")
  missing_dates <- expected_dates[!(expected_dates %in% merged[["date"]])]
  if (length(missing_dates) > 0) {
    print(paste("Missing dates (in merged data):",
                  paste(missing_dates, collapse = ", ")))
  }

  write.csv(merged, CSV_FPATH, row.names = FALSE)
  print("Finished: CSV was updated.")
}
