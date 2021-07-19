# Updates daily GNH CSV

library("stringr")

source("GNH/get_daily.R")


OUTPUT_FOLDER <- "GNH/output"

# Find most recent CSV, if it exists
previous <- list.files(OUTPUT_FOLDER, pattern = "gnh_daily_\\d{8}.csv") %>%
  str_match("gnh_daily_(\\d{8}).csv")
if (nrow(previous) > 0) {
  most_recent <- previous[which.max(previous[,2]), 1] %>%
    file.path(OUTPUT_FOLDER, .) %>%
    read_csv()
  start <- max(most_recent[["tweet_at"]]) + days(1)
} else {
  start <- ymd("2019-05-12")
}

if (start == today(tzone = "NZ") + days(1)) {
  print("Most recent CSV is up-to-date.")
} else {
  end <- today(tzone = "NZ") - days(1)
  print(c(start, end))
  new <- get_daily_gnh(start_date = start, end_date = end)

  # Check there is entries for every date (in new data)
  expected_dates <- seq.Date(from = start, to = end, by = "days")
  missing_dates <- expected_dates[!(expected_dates %in% new[["tweet_at"]])]
  if (length(missing_dates) > 0) {
    warning(paste("Missing dates (in new data):",
                  paste(missing_dates, collapse = ", ")))
  }

  # Export all data to new CSV
  if (!dir.exists("GNH/output")) {
    dir.create("GNH/output")
  }
  if (nrow(previous) > 0) {
    merged <- rbind(most_recent, new)
  } else {
    merged <- new  # there was no previous data
  }
  last_date_str <- format(max(new[["tweet_at"]]), "%Y%m%d")
  write.csv(merged, paste0("GNH/output/gnh_daily_", last_date_str, ".csv"),
            row.names = FALSE)
}
