#!/usr/bin/env Rscript

library(lubridate)
library(dplyr)
library(MASS)
library(fitdistrplus)
library(FAdist)
library(glm2)
library(arm)
library(tidyr)

args = commandArgs(trailingOnly=TRUE)
input <- args[1]
output <- args[2]
project.to <- as.Date(parse_date_time(args[3], "%Y-%m-%d"))
seed.long <- args[4]
set.seed(seed.long)

df <- read.csv(input, header = TRUE, stringsAsFactors = FALSE, na.strings ='')
df$beginning <- as.Date(parse_date_time(df$beginning, '%Y-%m-%d %H:%M:%S'))
df$admission_age <- as.character(df$admission_age)

floor_days <- function(date, max_date, granularity) {
    # Function assumes max_date is the end of the final group,
    # groups dates into ranges of `granularity` days.
    # Function maps each date to the middle of its corresponding range.
    ((max_date + 1) - ((as.numeric(max_date - date) %/% granularity) + 1) * granularity) + (granularity %/% 2)
}

max_date <- max(df$beginning)
granularity <- 28 * 3

dat <- df %>%
    mutate(quarter = floor_days(beginning, max_date, granularity )) %>%
    group_by(admission_age, quarter) %>%
    dplyr::summarise(n = n()) %>%
    ungroup %>%
    complete(admission_age, quarter, fill = list(n = 0)) %>%
    arrange(admission_age, quarter) %>%
    as.data.frame

dat <- dat[duplicated(dat$admission_age),] # Remove the first period, likely to be incomplete

mod <- bayesglm(n ~ quarter * admission_age, data = dat, family = poisson(link = "log"))
params <- mvrnorm(1, coef(mod), vcov(mod))
params.df <- data.frame(name = names(params), param = params)

# FIXME: override trending with static data
# Take a mean of arrivals
mean_arrivals <- dat %>%
    group_by(admission_age) %>%
    dplyr::mutate(c = n()) %>%
    sample_n(c, replace = TRUE) %>%
    dplyr::summarise(n = mean(n))

params.df <- data.frame(name = c("(Intercept)", "quarter",
                                 paste0("admission_age", mean_arrivals$admission_age),
                                 paste0("quarter:admission_age", mean_arrivals$admission_age)),
                        param = c(0, 0,
                                  log(mean_arrivals$n),
                                  rep(0, nrow(mean_arrivals))))

write.csv(params.df, output, row.names = FALSE)
