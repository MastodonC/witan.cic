#!/usr/bin/env Rscript

library(dplyr)
library(fitdistrplus)
library(reshape2)

args = commandArgs(trailingOnly=TRUE)
input <- args[1]

phase.duration.quantiles.out <- args[2]
phase.beta.params.out <- args[3]

phase_durations <- read.csv(input, header = TRUE, stringsAsFactors = FALSE, na.strings ='')

q_first <- quantile(phase_durations[phase_durations$first.phase == "true",]$phase.duration, probs = seq(0,1,length.out = 101))
q_rest <- quantile(phase_durations[phase_durations$first.phase == "false",]$phase.duration, probs = seq(0,1,length.out = 101))

quantiles <- rbind(cbind(quantile = 0:100, melt(q_first), label = "first"),
                   cbind(quantile = 0:100, melt(q_rest), label = "rest"))
write.csv(quantiles, phase.duration.quantiles.out, row.names = FALSE)

phases <- phase_durations %>%
  mutate(phase_p = phase.duration / total.duration)
beta_params <- data.frame(age = c(), alpha = c(), beta = c())
for (age.x in 0:17) {
  xs <- (phases %>% filter(age == age.x & phase_p > 0 & phase_p < 1))$phase_p
  fit <- fitdist(xs, "beta")
  beta_params <- rbind(beta_params, data.frame(age = age.x, alpha = fit$estimate["shape1"], beta = fit$estimate["shape2"]))
}

bernoulli_params <- phases %>% filter(!is.na(phase_p)) %>% group_by(age) %>% summarise(alpha = sum(phase_p != 1), beta = sum(phase_p == 1.0))

write.csv(beta_params, phase.beta.params.out, row.names = FALSE)
