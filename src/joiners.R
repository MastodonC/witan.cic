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
model.out <- args[2]
rates.out <- args[3]
seed.long <- args[4]
set.seed(seed.long)

df <- read.csv(input, header = TRUE, stringsAsFactors = FALSE, na.strings ='')
df$beginning <- as.Date(parse_date_time(df$beginning, 'ymd HMS'))
df$admission_age <- as.factor(as.character(df$admission_age))

diffs <- df %>%
    arrange(admission_age, beginning) %>%
    group_by(admission_age) %>%
    mutate(diff = interval(lag(beginning), beginning) / days(1), n = n()) %>%
    ungroup %>%
    filter(!is.na(diff) & n >= 3) %>% # We need at least 3 data points for each age to generate 2 diffs
    dplyr::select(admission_age, diff, beginning) %>%
    mutate(diff = diff + 0.01) %>% # Diff must always be greater than zero
    mutate(admission_age = droplevels(admission_age)) %>%
    as.data.frame

joiners.model <- bayesglm(diff ~ beginning * admission_age, data = diffs, family=Gamma(link = log))
mod.summary <- summary(joiners.model)

coef.samples <- mvrnorm(n = 2, mu = coefficients(joiners.model), Sigma = vcov(joiners.model))
colnames(coef.samples)[1] <- 'intercept'

write.csv(coef.samples, model.out, row.names=FALSE)

gamma.rates <- data.frame(admission_age = c(), shape = c(), rate = c())
for (i in as.character(0:17)) {
    interarrival <- (diffs %>% filter(admission_age == i))$diff
    fit <- fitdist(interarrival, 'gamma', lower = c(0, 0), start = list(scale = 1, shape = 1), method = 'mme')
    gamma.rates <- rbind(gamma.rates, data.frame(admission_age = i, shape = fit$estimate[1], rate = fit$estimate[2], dispersion = mod.summary$dispersion))
}

write.csv(gamma.rates, rates.out, row.names=FALSE)
