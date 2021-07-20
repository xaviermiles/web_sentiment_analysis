# Functions to load daily Gross National Happiness (GNH) data.

library(tidyverse)
library(lubridate)
library(httr)
library(rjson)

source("GNH/load_api_config.R")  # loads API_CONFIG (list)


get_daily_gnh_responses <- function(start_date, end_date) {
  # NB: Must request 7 days at a time - starts at end_date and moves backwards.
  FIRST_DATE <- ymd("2019-05-12")  # first date with data
  if (start_date > end_date) {
    stop("start_date should be before end_date.")
  } else if (end_date < FIRST_DATE) {
    stop("end_date should be >= 2019-05-12 (first date with data).")
  }
  start_date <- max(start_date, FIRST_DATE)
  end_date <- min(end_date, today(tzone = "NZ") - days(1)) # up until yesterday

  api_url <- paste0(API_CONFIG[["base_url"]], "GetDailySentiments")

  responses <- list()
  current_date <- end_date
  while (current_date >= start_date) {
    request_url <- parse_url(api_url)
    request_url$query <- list(
      fromDate = format(max(start_date, current_date - days(6)), "%Y-%m-%d"),
      toDate = format(current_date, "%Y-%m-%d")
    )
    response <- request_url %>%
      build_url() %>%
      GET(
        add_headers(
          Accept = "text/plain",
          ApiKey = API_CONFIG[["api_key"]]
        )
      ) %>%
      content(as = "text", encoding = "UTF-8") %>%
      fromJSON()

    responses[[length(responses) + 1]] <- response
    current_date <- current_date - weeks(1)
  }
  return(responses)
}

get_daily_gnh <- function(start_date = NULL, end_date = NULL) {
  # The expected parameters and associated types are specified in the
  # DailySentiment schema.
  if (is.null(end_date)) {
    end_date <- today(tzone = "NZ") - days(1)  # default: yesterday
  }
  if (is.null(start_date)) {
    start_date <- end_date - days(6)  # default: gives one week data
  }

  expected_parameters <- c(
    "country_code","tweet_at","tweet_at_year","tweet_at_month","tweet_at_day",
    "allTweets","totalPositiveTweets","totalNegativeTweets","gnh","gnH2"
  )

  daily_gnh <-
    get_daily_gnh_responses(start_date, end_date) %>%
    unlist(recursive = FALSE) %>%  # remove grouping of responses
    sapply(
      function(x) {
        x[sapply(x, is.null)] <- NA
        unlist(x)
      }
    ) %>%
    t() %>%
    as_tibble()
  if (!identical(colnames(daily_gnh), expected_parameters)) {
    stop("Recieved DailySentiment data does not contain expected parameters.")
  }
  daily_gnh_final <- daily_gnh %>%
    mutate(
      tweet_at = ymd(tweet_at),
      tweet_at_year = as.integer(tweet_at_year),
      tweet_at_month = as.integer(tweet_at_month),
      tweet_at_day = as.integer(tweet_at_day),
      allTweets = as.integer(allTweets),
      totalPositiveTweets = as.numeric(totalPositiveTweets),
      totalNegativeTweets = as.numeric(totalNegativeTweets),
      gnh = as.numeric(gnh),
      gnH2 = as.numeric(gnH2)
    ) %>%
    arrange(country_code, tweet_at)
  return(daily_gnh_final)
}
