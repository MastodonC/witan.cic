#!/usr/bin/env Rscript

library(lubridate)
library(dplyr)
library(MASS)
library(fitdistrplus)
library(FAdist)
library(glm2)
library(arm)

args = commandArgs(trailingOnly=TRUE)
input <- args[1]
output <- args[2]
project.to <- as.Date(args[3])
seed.long <- args[4]
set.seed(seed.long)

df <- read.csv(input, header = TRUE, stringsAsFactors = FALSE, na.strings ='')
df$beginning <- as.Date(parse_date_time(df$beginning, 'ymd HMS'))
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

mod <- bayesglm(n ~ quarter * admission_age, data = dat)
params <- mvrnorm(1, coef(mod), vcov(mod))

write.csv(data.frame(name = names(params), param = params), output, row.names = FALSE)

