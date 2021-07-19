# Functions to load daily Gross National Happiness (GNH) data.

library(tidyverse)
library(lubridate)
library(httr)
library(rjson)

source("GNH/load_api_config.R")  # loads API_CONFIG (list)


get_daily_gnh_responses <- function(num_weeks_limit) {
  # Must request 7 days at a time - starts one week ago (ie excludes current
  # date) and moves backwards until there is nothing being returned.
  # NB: first date with data is 2019-05-12 (ymd).
  if (num_weeks_limit %% 1 != 0 || num_weeks_limit < 1) {
    stop("num_weeks_limit should be a positive integer.")
  }

  api_url <- paste0(API_CONFIG[["base_url"]], "GetDailySentiments")

  start_date <- today()
  responses <- list()

  while (length(responses) < num_weeks_limit) {
    start_date <- start_date - days(7)
    request_url <- parse_url(api_url)
    request_url$query <- list(
      fromDate = start_date,
      toDate = start_date + days(6)
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

    if (length(response) > 0) {
      responses[[length(responses) + 1]] <- response
    } else {
      break
    }
  }
  return(responses)
}

get_daily_gnh <- function(num_weeks_limit = 1) {
  # The expected parameters and associated types are specified in the
  # DailySentiment schema.
  expected_parameters <- c(
    "country_code","tweet_at","tweet_at_year","tweet_at_month","tweet_at_day",
    "allTweets","totalPositiveTweets","totalNegativeTweets","gnh","gnH2"
  )

  daily_gnh <-
    get_daily_gnh_responses(num_weeks_limit) %>%
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
      tweet_at_day = as.integer(tweet_at_day),
      allTweets = as.integer(allTweets),
      totalPositiveTweets = as.numeric(totalPositiveTweets),
      totalNegativeTweets = as.numeric(totalNegativeTweets),
      gnh = as.numeric(gnh),
      gnH2 = as.numeric(gnH2)
    )
  return(daily_gnh_final)
}
