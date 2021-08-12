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
  complete(quarter, age, fill = list(simulation.id = 1, n.per.period = 0)) %>%
  mutate(mean.per.period = mean(n.per.period))

pdf(file = file.path(input_dir, "joiner-distribution-comparison.pdf"))
for (the.age in 0:17) {
  print(rbind(historic_joiner_summary_complete %>%
          select(simulation.id, quarter, age, n.per.period, mean.per.period) %>%
          mutate(label = "target"),
          projected_joiner_summary_complete %>%
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

# 2021-08-05

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-1/'

joiner_interval_log_csv <- "log/joiner-interval-log.csv"
joiner_interval_log <- read.csv(file.path(input_dir, joiner_interval_log_csv), na.strings = "")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day)) %>%
  mutate(avg.n.per.month = avg.n.per.day * 365.25 / 12.0)

# avg per month always seems about 10% too high

joiner_interval_log %>%
  filter(age == 0) %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  mutate(n.per.month = n.per.day * 365.25 / 12.0) %>%
  mutate(avg.n.per.month = mean(n.per.month)) %>%
  ggplot(aes(n.per.month)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.n.per.month), colour = "orange", linetype = 2)

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  ggplot(aes(interval.days, target.rate)) +
  geom_point(alpha = 0.1, position = "jitter") +
  facet_wrap(vars(age), scales = "free")

## Output 2 (average out per-period rate)

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-2/'

joiner_interval_log_csv <- "log/joiner-interval-log.csv"
joiner_interval_log <- read.csv(file.path(input_dir, joiner_interval_log_csv), na.strings = "")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day)) %>%
  mutate(avg.n.per.month = avg.n.per.day * 365.25 / 12.0)

# avg per month always seems about 10% too high

joiner_interval_log %>%
  filter(age == 0) %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  mutate(n.per.month = n.per.day * 365.25 / 12.0) %>%
  mutate(avg.n.per.month = mean(n.per.month)) %>%
  ggplot(aes(n.per.month)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.n.per.month), colour = "orange", linetype = 2)

###

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-3'

joiner_interval_log_csv <- "log/joiner-interval-log.csv"
joiner_interval_log <- read.csv(file.path(input_dir, joiner_interval_log_csv), na.strings = "")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day)) %>%
  mutate(avg.n.per.month = avg.n.per.day * 365.25 / 12.0)

# Exact as expected

joiner_interval_log %>%
  filter(age == 0) %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  mutate(n.per.month = n.per.day * 365.25 / 12.0) %>%
  mutate(avg.n.per.month = mean(n.per.month)) %>%
  ggplot(aes(n.per.month)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.n.per.month), colour = "orange", linetype = 2)

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  ggplot(aes(interval.days, target.rate)) +
  geom_point(alpha = 0.1, position = "jitter") +
  facet_wrap(vars(age), scales = "free")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  ggplot(aes(interval.days)) +
  geom_histogram() +
  facet_wrap(vars(age), scales = "free")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day), avg.interval.days = mean(interval.days)) %>%
  mutate(target.interval.days = 1 / avg.n.per.day)

# Let's verify the episodes match expectations too:

episodes_csv <- 'projection-episodes.csv'
episodes <- read.csv(file.path(input_dir, episodes_csv), na.strings = "")
periods <- episodes %>% filter(Episode == 1) %>%
  mutate(period.start = ymd(Period.Start), quarter = floor_quarter(period.start),
         month = floor_date(period.start, unit = "month"))

arrivals <- periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, quarter, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  group_by(Admission.Age) %>%
  mutate(avg.n.per.period = mean(n)) %>% ungroup

arrivals %>%
  ggplot(aes(n)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.n.per.period), colour = "orange", linetype = 2) +
  facet_wrap(vars(Admission.Age), scales = "free")

periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, quarter, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.period = mean(n)) %>%
  mutate(avg.n.per.month = avg.n.per.period / 84.0 * 365.25 / 12.0)

# Expectation vs median

# Expectation per month 49.5

library(reshape2)

periods %>%
  filter(Provenance == "S") %>%
  group_by(month, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  summarise(median.n.per.month = median(n), avg.n.per.month = mean(n)) %>%
  melt(id.vars = "month") %>%
  ggplot(aes(month, value, colour = variable)) +
  geom_line() +
  geom_hline(aes(yintercept = 49.5), colour = "orange", linetype = 2)

## Reintroduce poisson sampling

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-4'

joiner_interval_log_csv <- "log/joiner-interval-log.csv"
joiner_interval_log <- read.csv(file.path(input_dir, joiner_interval_log_csv), na.strings = "")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day)) %>%
  mutate(avg.n.per.month = avg.n.per.day * 365.25 / 12.0) %>%
  mutate(target.per.month = c(seq(5, 0.5, by = -0.5), seq(1, 4.5, by = 0.5))) %>%
  select(age, avg.n.per.month, target.per.month) %>%
  melt(id.vars = "age") %>%
  ggplot(aes(age, value, colour = variable))+
  geom_point()

# Always a difference. Try median.

poisson_samples <- read.csv("/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file13408022594446041069.csv")

poisson_samples %>%
  group_by(lambda) %>%
  summarise(avg.sample = mean(sample))

poisson_samples %>%
  group_by(lambda) %>%
  mutate(avg.sample = mean(sample)) %>%
  ggplot(aes(sample)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.sample), colour = "orange", linetype = 2) +
  facet_wrap(vars(lambda), scales = "free")

# Calculated mean is the same as lambda. Poisson appears to be working well.

# Check lambda-exponential

exponential_samples <- read.csv("/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file1214432200733729220.csv")

exponential_samples %>%
  group_by(lambda) %>%
  summarise(avg.sample = mean(sample)) %>%
  mutate(target.sample = 1 / lambda)

exponential_samples %>%
  group_by(lambda) %>%
  mutate(avg.sample = mean(sample)) %>%
  ggplot(aes(sample)) +
  geom_histogram(bins = 100) +
  geom_vline(aes(xintercept = avg.sample), colour = "orange", linetype = 2) +
  facet_wrap(vars(lambda), scales = "free")

# check poisson-exponential

poisson_exponential_samples <- read.csv("/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file6996248575498403732.csv")

poisson_exponential_samples %>%
  group_by(constrained.sample) %>%
  summarise(avg.exp.sample = mean(exponential.sample)) %>%
  mutate(target.sample = 1 / constrained.sample)

poisson_exponential_samples %>%
  group_by(poisson.sample) %>%
  summarise(avg.exp.sample = mean(exponential.sample)) %>%
  mutate(target.sample = 1 / poisson.sample)

poisson_exponential_samples %>%
  group_by(lambda) %>%
  summarise(avg.exp.sample = mean(exponential.sample)) %>%
  mutate(target.sample = 1 / lambda)


## Verify output matches input

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-1/'

historic_episodes_csv <- "historic-episodes.csv"
historic_episodes <- read.csv(file.path(input_dir, historic_episodes_csv), na.strings = "")

historic_episodes %>%
  filter(Episode == 1) %>%
  mutate(financial_year = financial_year(ymd(Period.Start))) %>%
  filter(financial_year %in% c("2016/17", "2017/18", "2018/19", "2019/20")) %>%
  group_by(financial_year) %>%
  summarise(n = n())


joiners_per_month <- historic_episodes %>%
  filter(Episode == 1) %>%
  mutate(financial_year = financial_year(ymd(Period.Start))) %>%
  filter(financial_year %in% c("2016/17", "2017/18", "2018/19", "2019/20")) %>%
  group_by(Admission.Age) %>%
  summarise(n = n()) %>%
  mutate(rate = n / 48.0)

sum(joiners_per_month$n) / 48

joiners_per_month$rate


## Plug in true rates


input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-5'

episodes_csv <- 'projection-episodes.csv'
episodes <- read.csv(file.path(input_dir, episodes_csv), na.strings = "")
periods <- episodes %>% filter(Episode == 1) %>%
  mutate(period.start = ymd(Period.Start), quarter = floor_quarter(period.start),
         financial_year = financial_year(period.start),
         month = floor_date(period.start, unit = "month"))


arrivals <- periods %>%
  filter(Provenance == "S") %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  summarise(avg.n.per.year = mean(n),
            median.n.per.year = median(n)) %>% ungroup

arrivals <- periods %>%
  filter(Provenance == "S") %>%
  group_by(financial_year) %>%
  summarise(n = n() / 100.0, .groups = "drop_last")

periods %>%
  filter(Provenance == "S" & financial_year %in% c("2022/23", "2023/24", "2024/25", "2025/26")) %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  ungroup %>%
  mutate(mean = mean(n)) %>%
  ggplot(aes(n)) +
  geom_histogram() +
  geom_vline(aes(xintercept = mean), colour = "orange", linetype = 2)

periods %>%
  filter(Provenance == "S") %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  filter(n < 50)

periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, quarter, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(Admission.Age = factor(Admission.Age), Simulation = factor(Simulation)) %>%
  complete(Admission.Age, quarter, Simulation, fill = list(n = 0)) %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.period = mean(n)) %>%
  mutate(avg.n.per.month = avg.n.per.period / 84.0 * 365.25 / 12.0)

joiner_interval_log_csv <- "log/joiner-interval-log.csv"
joiner_interval_log <- read.csv(file.path(input_dir, joiner_interval_log_csv), na.strings = "")

joiner_interval_log %>%
  mutate(age = factor(age)) %>%
  filter(interval.adjustment == 0) %>%
  group_by(age) %>%
  summarise(avg.n.per.day = mean(n.per.day)) %>%
  mutate(avg.n.per.month = avg.n.per.day * 365.25 / 12.0) %>%
  mutate(target.per.month = joiners_per_month$rate) %>%
  select(age, avg.n.per.month, target.per.month) %>%
  melt(id.vars = "age") %>%
  ggplot(aes(age, value, colour = variable))+
  geom_point(position = "jitter")


periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, month, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(Admission.Age = factor(Admission.Age), Simulation = factor(Simulation)) %>%
  complete(Admission.Age, month, Simulation, fill = list(n = 0)) %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.month = mean(n)) %>%
  mutate(target.n.per.month = joiners_per_month$rate) %>%
  select(Admission.Age, target.n.per.month, avg.n.per.month) %>%
  melt(id.vars = "Admission.Age") %>%
  ggplot(aes(Admission.Age, value, colour = variable)) +
  geom_point()
  

### 2019 projection ...

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-6'

episodes_csv <- 'projection-episodes.csv'
episodes <- read.csv(file.path(input_dir, episodes_csv), na.strings = "")
periods <- episodes %>% filter(Episode == 1) %>%
  mutate(period.start = ymd(Period.Start), quarter = floor_quarter(period.start),
         financial_year = financial_year(period.start),
         month = floor_date(period.start, unit = "month"))


periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, month, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(Admission.Age = factor(Admission.Age), Simulation = factor(Simulation)) %>%
  complete(Admission.Age, month, Simulation, fill = list(n = 0)) %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.month = mean(n)) %>%
  mutate(target.n.per.month = joiners_per_month$rate) %>%
  select(Admission.Age, target.n.per.month, avg.n.per.month) %>%
  melt(id.vars = "Admission.Age") %>%
  ggplot(aes(Admission.Age, value, colour = variable)) +
  geom_point()

periods %>%
  filter(Provenance == "S" & financial_year %in% c("2022/23", "2023/24", "2024/25", "2025/26")) %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  ungroup %>%
  mutate(mean = mean(n)) %>%
  ggplot(aes(n)) +
  geom_histogram() +
  geom_vline(aes(xintercept = mean), colour = "orange", linetype = 2)


## Joiner inference

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-7'
episodes <- read.csv(file.path(input_dir, episodes_csv), na.strings = "")
periods <- episodes %>% filter(Episode == 1) %>%
  mutate(period.start = ymd(Period.Start), quarter = floor_quarter(period.start),
         financial_year = financial_year(period.start),
         month = floor_date(period.start, unit = "month"))

joiner_rates_log_csv <- "log/joiner-rates-log.csv"
joiner_rates_log <- read.csv(file.path(input_dir, joiner_rates_log_csv), na.strings = "")

joiner_rates_log %>%
  mutate(n.per.month = n.per.day * 365.25 / 12.0) %>%
  group_by(age) %>%
  summarise(avg.per.month = mean(n.per.month),
            l50 = quantile(n.per.month, probs = 0.25),
            u50 = quantile(n.per.month, probs = 0.75), 
            l95 = quantile(n.per.month, probs = 0.025),
            u95 = quantile(n.per.month, probs = 0.975)) %>%
  mutate(expected = joiners_per_month$rate) %>%
  ggplot() +
  geom_segment(aes(x = age, y = l95, xend = age, yend = u95)) +
  geom_segment(aes(x = age, y = l50, xend = age, yend = u50), size = 1) +
  geom_point(aes(age, avg.per.month), size = 2) +
  geom_point(aes(age, expected), color = "red", size = 2)
3 * 28
joiner_rates_log %>%
  mutate(n.per.month = y.per.period / 84.0 * 365.25 / 12.0) %>%
  group_by(age) %>%
  summarise(avg.per.month = mean(n.per.month),
            l50 = quantile(n.per.month, probs = 0.25),
            u50 = quantile(n.per.month, probs = 0.75), 
            l95 = quantile(n.per.month, probs = 0.025),
            u95 = quantile(n.per.month, probs = 0.975)) %>%
  mutate(expected = joiners_per_month$rate) %>%
  ggplot() +
  geom_segment(aes(x = age, y = l95, xend = age, yend = u95)) +
  geom_segment(aes(x = age, y = l50, xend = age, yend = u50), size = 1) +
  geom_point(aes(age, avg.per.month), size = 2) +
  geom_point(aes(age, expected), color = "red", size = 2)

periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, month, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(Admission.Age = factor(Admission.Age), Simulation = factor(Simulation)) %>%
  complete(Admission.Age, month, Simulation, fill = list(n = 0)) %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.month = mean(n)) %>%
  mutate(target.n.per.month = joiners_per_month$rate) %>%
  select(Admission.Age, target.n.per.month, avg.n.per.month) %>%
  melt(id.vars = "Admission.Age") %>%
  ggplot(aes(Admission.Age, value, colour = variable)) +
  geom_point()

periods %>%
  filter(Provenance == "S" & financial_year %in% c("2022/23", "2023/24", "2024/25", "2025/26")) %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  ungroup %>%
  mutate(mean = mean(n)) %>%
  ggplot(aes(n)) +
  geom_histogram() +
  geom_vline(aes(xintercept = mean), colour = "orange", linetype = 2)

## Output 8 - remove poisson from untrended

input_dir <- '/Users/henry/Mastodon C/witan.cic/data/bwd/2021-08-05/output-8'
episodes <- read.csv(file.path(input_dir, episodes_csv), na.strings = "")
periods <- episodes %>% filter(Episode == 1) %>%
  mutate(period.start = ymd(Period.Start), quarter = floor_quarter(period.start),
         financial_year = financial_year(period.start),
         month = floor_date(period.start, unit = "month"))

joiner_rates_log_csv <- "log/joiner-rates-log.csv"
joiner_rates_log <- read.csv(file.path(input_dir, joiner_rates_log_csv), na.strings = "")

joiner_rates_log %>%
  mutate(n.per.month = n.per.day * 365.25 / 12.0) %>%
  group_by(age) %>%
  summarise(avg.per.month = mean(n.per.month),
            l50 = quantile(n.per.month, probs = 0.25),
            u50 = quantile(n.per.month, probs = 0.75), 
            l95 = quantile(n.per.month, probs = 0.025),
            u95 = quantile(n.per.month, probs = 0.975)) %>%
  mutate(expected = joiners_per_month$rate) %>%
  ggplot() +
  geom_segment(aes(x = age, y = l95, xend = age, yend = u95)) +
  geom_segment(aes(x = age, y = l50, xend = age, yend = u50), size = 1) +
  geom_point(aes(age, avg.per.month), size = 2) +
  geom_point(aes(age, expected), color = "red", size = 2)

joiner_rates_log %>%
  mutate(n.per.month = y.per.period / 84.0 * 365.25 / 12.0) %>%
  group_by(age) %>%
  summarise(avg.per.month = mean(n.per.month),
            l50 = quantile(n.per.month, probs = 0.25),
            u50 = quantile(n.per.month, probs = 0.75), 
            l95 = quantile(n.per.month, probs = 0.025),
            u95 = quantile(n.per.month, probs = 0.975)) %>%
  mutate(expected = joiners_per_month$rate) %>%
  ggplot() +
  geom_segment(aes(x = age, y = l95, xend = age, yend = u95)) +
  geom_segment(aes(x = age, y = l50, xend = age, yend = u50), size = 1) +
  geom_point(aes(age, avg.per.month), size = 2) +
  geom_point(aes(age, expected), color = "red", size = 2)

periods %>%
  filter(Provenance == "S") %>%
  group_by(Admission.Age, month, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  mutate(Admission.Age = factor(Admission.Age), Simulation = factor(Simulation)) %>%
  complete(Admission.Age, month, Simulation, fill = list(n = 0)) %>%
  group_by(Admission.Age) %>%
  summarise(avg.n.per.month = mean(n)) %>%
  mutate(target.n.per.month = joiners_per_month$rate) %>%
  select(Admission.Age, target.n.per.month, avg.n.per.month) %>%
  melt(id.vars = "Admission.Age") %>%
  ggplot(aes(Admission.Age, value, colour = variable)) +
  geom_point()

periods %>%
  filter(Provenance == "S" & financial_year %in% c("2022/23", "2023/24", "2024/25", "2025/26")) %>%
  group_by(financial_year, Simulation) %>%
  summarise(n = n(), .groups = "drop_last") %>%
  ungroup %>%
  mutate(mean = mean(n)) %>%
  ggplot(aes(n)) +
  geom_histogram() +
  geom_vline(aes(xintercept = mean), colour = "orange", linetype = 2)
