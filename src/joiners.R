#!/usr/bin/env Rscript

library(lubridate)
library(dplyr)
library(MASS)
library(fitdistrplus)
library(FAdist)
library(glm2)
library(arm)

args = commandArgs(trailingOnly=TRUE)
# args = c("/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file14146548989716177472.csv","/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file346105388764673154.csv","2023-01-01","907901448")
input <- args[1]
output <- args[2]
project.to <- as.Date(parse_date_time(args[3], "%Y-%m-%d"))
seed.long <- args[4]
set.seed(seed.long)

df <- read.csv(input, header = TRUE, stringsAsFactors = FALSE, na.strings ='')
df$beginning <- as.Date(parse_date_time(df$beginning, '%Y-%m-%d %H:%M:%S'))
df$admission_age <- as.factor(as.character(df$admission_age))

quarters.between <- function(from, to) {
    seq(floor_date(min(from), "3 months"),
        floor_date(max(to), "3 months"),
        "3 months")
}

# Ensure every age is represented every quarter
defaults <- expand.grid(quarter = quarters.between(min(df$beginning), max(df$beginning)), admission_age = levels(df$admission_age))

# Count the joiners per age and quarter
dat <- df %>%
    mutate(quarter = floor_date(beginning, "3 months")) %>%
    group_by(admission_age, quarter) %>%
    summarise(n = n()) %>%
    as.data.frame

# Data including zero counts
dat <- defaults %>%
    left_join(dat) %>%
    mutate(n = coalesce(n, as.integer(0)))

mod <- bayesglm(n ~ quarter * admission_age, data = dat, family = poisson(link = "log"))
params <- mvrnorm(1, coef(mod), vcov(mod))
params.df <- data.frame(name = names(params), param = params)

# rand <- as.integer(runif(1, 1000, 9999))
# write.csv(df, sprintf("/Users/henry/Mastodon C/witan.cic/data/testing/input-%s.csv", rand), row.names = FALSE)
# write.csv(params.df, sprintf("/Users/henry/Mastodon C/witan.cic/data/testing/params-%s.csv", rand), row.names = FALSE)

write.csv(params.df, output, row.names = FALSE)

