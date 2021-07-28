library(dplyr)
source("helpers.R")

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-07-21/outputs/2021-train-pre-covid'

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
  dplyr::summarise(n = n() / month_range) %>%
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
