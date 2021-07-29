library(dplyr)
library(tidyr)
source("helpers.R")


input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-07-21/outputs/2021-baseline-untrended'

historic_episodes_file <- "historic-episodes.csv"
historic_episodes <- read.csv(file.path(input_dir, historic_episodes_file), na.strings = "") %>%
  group_by(ID) %>% slice(1)
historic_episodes$Period.Start <- as.Date(historic_episodes$Period.Start)

# Calculate monthly joiners per age within range

start <- as.Date("2015-04-01")
end <- as.Date("2020-04-01")
day_range <- day_diff(start, end)
month_range <- day_range / 365.25 * 12


historic_rates <- historic_episodes %>%
  filter(Period.Start >= start & Period.Start < end) %>%
  group_by(Admission.Age) %>%
  dplyr::summarise(n = n() / day_range) %>%
  mutate(p = n / sum(n))

historic_rate <- historic_episodes %>%
  filter(Period.Start >= start & Period.Start < end) %>%
  mutate(month = format(Period.Start, "%Y-%m")) %>%
  group_by(month) %>%
  dplyr::summarise(n = n(), .groups = "drop_last") %>%
  dplyr::summarise(md = median(n), mu = mean(n))
  

start <- as.Date("2020-04-01")
end <- as.Date("2021-04-01")
day_range <- day_diff(start, end)
month_range <- day_range / 365.25 * 12

covid_rates <- historic_episodes %>%
  filter(Period.Start >= start & Period.Start < end) %>%
  group_by(Admission.Age) %>%
  dplyr::summarise(n = n() / month_range)

covid_rate <- historic_episodes %>%
  filter(Period.Start >= start & Period.Start < end) %>%
  mutate(month = format(Period.Start, "%Y-%m")) %>%
  group_by(month) %>%
  dplyr::summarise(n = n()) %>%
  dplyr::summarise(md = median(n), mu = mean(n))

historic_rates %>%
  left_join(covid_rates, by = "Admission.Age") %>%
  mutate(n.y = coalesce(n.y, 0)) %>%
  mutate(catchup_rate = n.x + (n.x - n.y))


rates <- read.csv("/tmp/witan.cic.rates.csv") %>%
  filter(admission_age != "admission_age") %>%
  group_by(admission_age) %>%
  mutate(n = as.numeric(n)) %>%
  dplyr::summarise(n = mean(n) / 3.0)




historic_rate %>%
  left_join(covid_rate, by = character()) %>%
  mutate(md.y = coalesce(md.y, 0)) %>%
  mutate(normal_rate = md.x, catchup_rate = md.x + (md.x - md.y)) %>%
  inner_join(historic_rates, by = character(0)) %>%
  mutate(normal_rate_per_age = normal_rate * p,
         catchup_rate_per_age = catchup_rate * p) %>%
  dplyr::select(Admission.Age, normal_rate_per_age, catchup_rate_per_age) %>%
  write.csv("rates.csv")

catchup_rates <- data.frame(Admission.Age = 0:17, n = historic_rates$n + (historic_rates$n - covid_rates$n))


input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-07-28/outputs/2021-baseline-untrended'

joiner_rates <- read.csv(file.path(input_dir, "log", "joiner-rates-log.csv")) %>%
  mutate(quarter = ymd(period.from), period.to = ymd(period.to), n.per.period = n.per.day * 84) %>%
  group_by(quarter, age) %>%
  mutate(mean.per.period = mean(n.per.period))
projected_episodes <- read.csv(file.path(input_dir, "projection-episodes.csv"))

rate_start <- min(joiner_rates$period.from)

floor_quarter <- function(date) {
  rate_start + days((day_diff(rate_start, date) %/% 84) * 84)
}

ggplot(joiner_rates, aes(n.per.day)) +
  geom_histogram(bins = 100) +
  facet_grid(vars(age))

joiner_rates %>%
  group_by(age) %>%
  summarise(m = mean(n.per.day))

data.frame(xs = rpois(10000, 0.104 * 28)) %>%
  ggplot(aes(xs)) +
  geom_histogram()

projected_periods <- projected_episodes %>%
  group_by(ID) %>%
  slice(1) %>%
  ungroup %>%
  mutate(simulation.id = Simulation, beginning = ymd(Period.Start), end = ymd(Period.End), age = Admission.Age) %>%
  select(ID, simulation.id, beginning, end, age)


projected_joiner_summary <- projected_periods %>%
  mutate(quarter = floor_quarter(beginning)) %>%
  filter(quarter >= rate_start) %>%
  group_by(quarter, age, simulation.id) %>%
  summarise(n.per.period = n(), .groups = "drop_last")

projected_joiner_summary_complete <- projected_joiner_summary %>%
  mutate(age = factor(age), simulation.id = factor(simulation.id)) %>%
  complete(quarter, age, simulation.id, fill = list(n.per.period = 0)) %>%
  group_by(quarter, age) %>%
  mutate(mean.per.period = mean(n.per.period)) %>%
  mutate(age = as.integer(as.character(age)), simulation.id = as.character(simulation.id))

historic_joiner_summary <- projected_periods %>%
  mutate(quarter = floor_quarter(beginning)) %>%
  filter(quarter < rate_start & simulation.id == 1) %>%
  group_by(quarter, age, simulation.id) %>%
  summarise(n.per.period = n(), .groups = "drop_last") %>%
  ungroup

historic_joiner_summary_complete <- historic_joiner_summary %>%
  ungroup %>%
  mutate(age = factor(age)) %>%
  complete(quarter, age, fill = list(simulation.id = 1, n.per.period = 0))

pdf(file = file.path(input_dir, "joiner-distribution-comparison.pdf"))
for (the.age in 0:17) {
  print(rbind(joiner_rates %>%
          select(simulation.id, quarter, age, n.per.period, mean.per.period) %>%
          mutate(label = "target"),
        joiner_summary %>%
          select(simulation.id, quarter, age, n.per.period, mean.per.period) %>%
          mutate(label = "simulated")) %>%
    filter(age == the.age) %>%
    ggplot(aes(label, n.per.period, fill = label)) +
    geom_hline(aes(yintercept = mean.per.period, colour = label)) +
    geom_violin() +
    facet_wrap(vars(quarter)) +
    labs(title = paste("Age", the.age, "comparison of target and simulated joiner rate distribution")))
}
dev.off()


projected_periods 

projected_joiner_summary_complete %>%
  filter(age == 0) %>%
  ggplot(aes(quarter, n.per.period)) +
  geom_point(aes(colour = simulation.id), position = "jitter")

projected_joiner_summary_complete %>%
  filter(age == 0) %>%
  ggplot(aes(quarter, n.per.period)) +
  stat_density2d_filled()

pdf(file = file.path(input_dir, "joiner-distribution-comparison-2.pdf"))
for (the.age in 0:17) {
  projected <- projected_joiner_summary_complete %>%
    filter(age == the.age) %>%
    mutate(quarter = factor(quarter))
  historic <- historic_joiner_summary_complete %>%
    filter(age == the.age) %>%
    filter(quarter > as.Date("2010-01-01")) %>%
    mutate(quarter = factor(quarter))
  
  print(ggplot(data = NULL, aes(quarter, n.per.period)) +
    geom_violin(aes(), data = historic %>% mutate(quarter = factor(as.Date("2012-04-11")))) +
    geom_violin(aes(), data = projected) +
    geom_point(aes(), data = historic) +
      labs(title = paste("Age", the.age)))
}
dev.off()



pdf(file = file.path(input_dir, "joiner-distribution-comparison-poisson.pdf"))
for (the.age in 0:17) {
  projected_rates <- joiner_rates %>%
    ungroup %>%
    filter(age == the.age & quarter >= as.Date("2015-04-01")) %>%
    mutate(mean = mean(n.per.period)) %>%
    group_by(mean, n.per.period) %>%
    summarise(n = n())
  
  projected <- projected_joiner_summary_complete %>%
    ungroup %>%
    filter(age == the.age) %>%
    mutate(mean = mean(n.per.period)) %>%
    group_by(mean, n.per.period) %>%
    summarise(n = n())
  historic <- historic_joiner_summary_complete %>%
    filter(age == the.age) %>%
    filter(quarter >= as.Date("2015-04-01")) %>%
    mutate(mean = mean(n.per.period)) %>%
    group_by(mean, n.per.period) %>%
    summarise(n = n())
  
  print(ggplot(data = rbind(projected %>% mutate(label = "projected"),
                            historic %>% mutate(label = "historic")),
               aes(n.per.period, n)) +
          geom_bar(stat = "identity") +
          geom_vline(aes(xintercept = mean), color = "orange", linetype = 2) +
          facet_wrap(vars(label), scales = "free_x") +
          coord_flip() +
          labs(title = paste("Age", the.age)))
}
dev.off()


