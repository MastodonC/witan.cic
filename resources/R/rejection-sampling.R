library(here)
library(lubridate)
library(survival)
library(survminer)
library(stringr)
library(reshape2)
library(zoo)
library(dplyr)
library(tidyr)
library(ggthemes)
library(RSQLite)

eighteen_years = 6575 # in days

args = commandArgs(trailingOnly=TRUE)
input_csv <- args[1]
periods_universe_csv <- args[2]
extract_date <- as.Date(parse_date_time(args[3], "%Y-%m-%d"))
output_csv <- args[4]
age_out_proportions_output_csv <- args[5]
simulated_output_csv <- args[6]
projected_output_csv <- args[7]
simulated_age_out_output_csv <- args[8]
projected_age_out_output_csv <- args[9]
charts_output_directory <- args[10]
seed.long <- args[11]
helper_file <- args[12]

set.seed(seed.long)
source(helper_file)

data <- read.csv(input_csv)
data$report_date <- ymd(data$report_date)
data$ceased <- ymd(data$ceased)


pdf(file = file.path(charts_output_directory, "ensure-no-date-outliers.pdf"))

data.frame(xs = c(data$report_date, data$ceased)) %>%
  filter(!is.na(xs)) %>%
  filter(xs > Sys.Date() - years(1)) %>%
    ggplot(aes(xs)) + geom_histogram()

dev.off()

# Fix dodgy NAs

# data$ceased[data$ceased > extract_date] <- NA
# data <- data[data$report_date <= extract_date,]
#
# data %>% filter(period_id == "881-1")
# data %>% filter(period_id == "486-1")
#
# data %>% filter(period_id == "1815-1")


# Creates data that will be fed to the survival curve fitting model.
# We only know which month each child's birthday falls in, so we create a record
# for each day of this month.

noise <- 7

survival_data <- data %>%
  mutate(report_date = ymd(report_date),
         ceased = ymd(ceased)) %>%
  group_by(period_id) %>%
  dplyr::summarise(period_start = min(report_date),
                   period_end = max(ceased),
                   birth_month_beginning = ymd(paste0(DOB[1], "-01")),
                   birth_month_end = birth_month_beginning + months(1) - days(1))

# We think the child aged out if they *could* have hit 18 years - 1 day given their birthday range
# That means if we assume the oldest they could be, did that cross 18 years - 1 day.
# That means if we take their earlier birthday, is it *at least* 18 years - 1 day.

survival_data2 <- survival_data %>%
  mutate(birthday_lower = pmax(birth_month_beginning, eighteen_years_before(replace_na(period_end, extract_date))),
         birthday_upper = pmin(birth_month_end, period_start),
         # aged_out = year_diff(birth_month_beginning, replace_na(period_end, extract_date)) >= 18,
         # Removing this period_end assignment - don't think we need it
         # period_end = if_else(aged_out, eighteen_years_after(birthday_upper) - days(1), period_end),
         event = !is.na(period_end),
         duration = if_else(event,
                            day_diff(period_start, period_end),
                            day_diff(period_start, extract_date)))

# These cases violate one of more of our assumptions. We'll assume they left at 18.
survival_data2 %>% filter(birthday_lower > birthday_upper)

survival_data3 <- survival_data2 %>%
  mutate(birthday_lower = pmin(birthday_lower, birthday_upper) # Only matters if there are age 18+ leavers above
         ) %>%
  inner_join(data.frame(n = 1:100), by = character()) %>%
  mutate(birthday = as.Date(impute_birthday(birthday_lower, birthday_upper), origin = "1970-01-01"),
         admission_age = as.factor(clamp(year_diff(birthday, period_start), 0, 17)),
         admission_age_days = day_diff(birthday, period_start),
         max_duration = day_diff(period_start, eighteen_years_after(birthday)),
         duration = pmin(duration, max_duration),
         exit_age = duration + admission_age_days,
         fuzzed_duration = binomial_noise(duration, max_duration, noise),
         fuzzed_exit_age = fuzzed_duration + admission_age_days,
         ) %>%
  as.data.frame

survival_data4 <- survival_data3 %>%
  mutate(aged_out = year_diff(birthday, period_start + days(fuzzed_duration)) >= 17, # Going to say no one leaves aged 17
         event = event | aged_out, # All aged out children are leavers
         duration = if_else(aged_out, max_duration, duration),
         fuzzed_duration = if_else(aged_out, max_duration, fuzzed_duration),
         fuzzed_exit_age = fuzzed_duration + day_diff(birthday, period_start))

fit <- survfit(Surv(fuzzed_duration, event) ~ admission_age, data = survival_data4 %>% filter(!aged_out))
quantiles <- reshape2::melt(stats::quantile(fit, probs = seq(0,0.9999,length.out = 10000))$quantile) %>%
  mutate(join_age = as.integer(str_replace(Var1, "admission_age=", ""))) %>%
  select(-1) %>%
  as.data.frame
colnames(quantiles) <- c("quantile", "duration", "admission_age")

quantiles <- dcast(admission_age ~ quantile, data = quantiles, drop = FALSE, value.var = "duration")
for (row in 1:nrow(quantiles)) {
  # I expect there's a better way of doing this
  age <- row - 1
  max_days <- (18 - age) * 365
  quantiles[row,is.na(quantiles[row,])] <- max_days
  quantiles[row,quantiles[row,] > max_days] <- max_days
}
quantiles <- cbind(quantiles, data.frame(`100` = 17:1*365))
colnames(quantiles) <- c("admission_age", seq(0, 10000, length.out = ncol(quantiles) - 1))
quantiles <- melt(quantiles, id.vars = "admission_age") %>%
  mutate(quantile = as.numeric(variable) / 10000.0,
         duration = value) %>%
  select(admission_age, duration, quantile) %>%
  group_by(admission_age, duration) %>%
  summarise(quantile = max(quantile))

target_distribution <- quantiles %>%
  mutate(duration_group = duration %/% 7) %>%
  group_by(admission_age, duration_group) %>%
  summarise(quantile = max(quantile)) %>%
  arrange(admission_age, duration_group) %>%
  mutate(density = quantile - coalesce(lag(quantile), 0)) %>%
  mutate(density = density * 100 / sum(density))

write.csv(target_distribution, file = output_csv, row.names = FALSE)

# Calculate probability that joiner of each age makes it past 17th birthday

fit <- survfit(Surv(fuzzed_exit_age, event) ~ admission_age, data = survival_data4)
quantiles <- reshape2::melt(stats::quantile(fit, probs = seq(0,1,length.out = 10000))$quantile) %>%
  mutate(join_age = as.integer(str_replace(Var1, "admission_age=", ""))) %>%
  select(-1) %>%
  as.data.frame
colnames(quantiles) <- c("quantile", "exit_age", "admission_age")

pdf(file = file.path(charts_output_directory, "ensure-age-out-peak-after-threshold.pdf"))

ggplot(quantiles %>% mutate(admission_age = factor(admission_age)), aes(exit_age, quantile)) +
  geom_line() +
  facet_wrap(vars(admission_age)) +
  geom_vline(xintercept = eighteen_years, linetype = 2, alpha = 0.2) +
  coord_cartesian(xlim = c(5000, 7000))

dev.off()

age_out_proportions <- quantiles %>%
  arrange(admission_age) %>%
  group_by(admission_age) %>%
  mutate(age_out_quantile = min(if_else(exit_age > eighteen_years, quantile, NA_real_), na.rm = TRUE)) %>%
  mutate(age_out_quantile = if_else(is.infinite(age_out_quantile), 100, age_out_quantile)) %>%
  mutate(p = (100 - age_out_quantile) / (100 - quantile)) %>%
  mutate(p = pmin(p, 1.0)) %>%
  group_by(admission_age, exit_age) %>%
  slice(1) %>%
  rename(current_age_days = exit_age)

pdf(file = file.path(charts_output_directory, "leave-age-distribution-before-age-out.pdf"))

ggplot(age_out_proportions, aes(current_age_days, p, group = admission_age)) +
  geom_line() +
  facet_wrap(vars(admission_age))

dev.off()

write.csv(age_out_proportions, file = age_out_proportions_output_csv, row.names = FALSE)

db <- dbConnect(RSQLite::SQLite(), "mastodon.sqlite")

# Upload periods universe
periods_universe <- read.csv(periods_universe_csv)

staged <- periods_universe
colnames(staged) <- c("provenance", "id", "sample_index", "admission_age", "admission_age_days", "duration", "episodes_edn", "aged_out")
dbWriteTable(db, "periods", staged, overwrite = TRUE, row.names = FALSE)

# Upload target distribution
staged <- target_distribution[,c("admission_age", "duration_group", "density")]
dbWriteTable(db, "target_distribution", staged, overwrite = TRUE, row.names = FALSE)

## We create versions of the input periods to try and get good coverage of the whole target distribution.
## This means adjusting the total duration by up to 10 weeks either way.
## We don't filter periods which go over 18 because we adjust the birthdays in the following section.

noise <- data.frame(x = (seq(-10, 10) * 7))
dbWriteTable(db, "noise", noise, overwrite = TRUE, row.names = FALSE)

dbExecute(db, "DROP TABLE IF EXISTS fuzzy_periods;")

dbExecute(db, "CREATE TABLE fuzzy_periods AS
WITH x AS (
  SELECT x FROM noise
)
SELECT p.provenance, p.id, p.sample_index, p.admission_age, p.admission_age_days,
  cast(p.duration + x AS int) AS duration, CAST(duration + x AS int) / 7 AS duration_group, episodes_edn
FROM periods AS p
  CROSS JOIN x
WHERE (duration + x >= 28 OR x = 0)
AND aged_out = 'false';")

dbExecute(db, "INSERT INTO fuzzy_periods
WITH x AS (
  SELECT x FROM noise
)
SELECT p.provenance, p.id, p.sample_index, p.admission_age, p.admission_age_days,
  cast(p.duration + x AS int) AS duration, CAST(duration + x AS int) / 7 AS duration_group, episodes_edn
FROM periods AS p
  CROSS JOIN x
WHERE duration + x >= 0
AND duration + x < 28
AND x != 0
AND aged_out = 'false';")

# For all those who join aged at least 1, we create versions of all simulated and projected data with fuzzed birthdays: up to 5 weeks in either direction.

noise <- data.frame(x = (seq(-5, 5) * 7))
dbWriteTable(db, "noise", noise, overwrite = TRUE, row.names = FALSE)

dbExecute(db, "WITH x AS (
  SELECT x FROM noise
)
INSERT INTO fuzzy_periods
SELECT p.provenance, p.id, p.sample_index, CAST(p.admission_age_days + x AS int) / 365, CAST(p.admission_age_days + x AS int) AS admission_age_days, p.duration, p.duration_group, episodes_edn
FROM fuzzy_periods AS p
CROSS JOIN x
WHERE x != 0 AND CAST(p.admission_age_days + x AS int) / 365 >= 1
AND provenance IN ('S', 'P');")

dbExecute(db, "DROP TABLE IF EXISTS candidate_distributions;")

dbExecute(db, "DROP TABLE IF EXISTS target_projected_distribution;")

dbExecute(db, "CREATE TABLE candidate_distributions AS
SELECT provenance, admission_age, duration_group, COUNT(*) AS density
FROM fuzzy_periods
GROUP BY provenance, admission_age, duration_group;")

dbExecute(db, "CREATE TABLE target_projected_distribution AS
WITH open_period_durations AS (
  SELECT *, CAST(duration AS int) / 7 AS duration_group
  FROM periods
  WHERE provenance = 'C'
),
tpd AS (
  SELECT pp.admission_age, td.duration_group, CASE WHEN pp.duration_group = td.duration_group THEN density * (7 - (pp.duration % 7)) ELSE density END AS density
  FROM open_period_durations pp
  INNER JOIN target_distribution td
  ON pp.admission_age = td.admission_age
  AND td.duration_group >= pp.duration_group
)
SELECT admission_age, duration_group, SUM(density) AS density
FROM tpd
GROUP BY admission_age, duration_group;")

## We need to ensure that every ID to be projected has at least one period with a non-zero density in the target (otherwise it will never be chosen)

target_projected_duration_groups <- dbGetQuery(db, "SELECT admission_age, duration_group, density
                                  FROM target_projected_distribution")

pdf(file = file.path(charts_output_directory, "target-duration-distributions.pdf"))

for (age in 0:16) {
  print(ggplot(target_projected_duration_groups %>% filter(admission_age == age), aes(duration_group, density)) +
    geom_bar(stat = "identity") +
    facet_wrap(vars(admission_age), scales = "free_y"))
}

dev.off()

non_join_duration_groups <- dbGetQuery(db, "WITH empty_ids AS (
SELECT fp.id
FROM fuzzy_periods fp
LEFT OUTER JOIN target_projected_distribution pd
ON fp.admission_age = pd.admission_age
AND fp.duration_group = pd.duration_group
WHERE fp.provenance = 'P'
GROUP BY fp.id
HAVING SUM(coalesce(pd.density, 0)) = 0
)SELECT id, admission_age, duration_group, COUNT(*) AS density
FROM fuzzy_periods
WHERE id IN (SELECT id FROM empty_ids)
AND provenance = 'P'
GROUP BY id, admission_age, duration_group;")

combined_duration_groups <- rbind(cbind(id = "target", target_projected_duration_groups),
      non_join_duration_groups) %>%
  group_by(id, admission_age) %>%
  mutate(density = density / max(density))

pdf(file = file.path(charts_output_directory, "flag-cases-with-no-duration-match.pdf"))

for (age in 0:16) {
  print(ggplot(combined_duration_groups %>% filter(admission_age == age), aes(duration_group, density, fill = id)) +
  geom_bar(stat = "identity", position = "fill") +
  facet_wrap(vars(admission_age)) +
  scale_fill_manual(values = tableau_color_pal("Tableau 20")(20)))
}

dev.off()

## Shows they should all remain until 18

dbExecute(db, "SELECT *
  FROM fuzzy_periods
  WHERE provenance = 'S'
  AND admission_age = 17;")

dbExecute(db, "DROP TABLE IF EXISTS simulated_candidates;")

dbExecute(db, "DROP TABLE IF EXISTS projected_candidates;")

dbExecute(db, "-- Update destination if necessary
CREATE TABLE projected_candidates AS
WITH source_periods AS (
  -- Update source periods if necessary
  SELECT *
  FROM fuzzy_periods
  WHERE provenance = 'P'
),
target_distribution AS (
  -- Update target if necessary
  SELECT * FROM target_projected_distribution
),
candidate_distribution AS (
  SELECT admission_age, duration_group, COUNT(*) AS density
  FROM source_periods
  GROUP BY admission_age, duration_group
),
reject_proportions AS (
  SELECT t.admission_age, t.duration_group, t.density / c.density AS reject_ratio
  FROM target_distribution t
  LEFT JOIN candidate_distribution c
  ON c.admission_age = t.admission_age
  AND c.duration_group = t.duration_group
)
SELECT fp.*, rp.reject_ratio
FROM source_periods fp
INNER JOIN reject_proportions rp
ON fp.admission_age = rp.admission_age
AND fp.duration_group = rp.duration_group
WHERE random() <= 0.25;")

## If no samples from a particular candidate made it through rejection sampling,
## bypass rejection sampling to add them in, because otherwise they will be dropped completely.

dbExecute(db, "INSERT INTO projected_candidates
WITH missing_periods AS (
SELECT *, row_number() OVER (PARTITION BY id ORDER BY random()) AS rand_id
FROM fuzzy_periods fp
WHERE fp.provenance = 'P'
AND id NOT IN (
  SELECT DISTINCT id
  FROM projected_candidates
)
)
-- Maybe we ought to set the duration to cause them to age out at 18 - this seem like their destiny.
-- However this would cause them to remain in their final placement for a long time.
SELECT provenance, id, sample_index, admission_age, admission_age_days, duration, duration_group, episodes_edn, 1.0 AS reject_ratio
FROM missing_periods
WHERE rand_id <= 100;")

## Update destination if necessary
dbExecute(db, "CREATE TABLE simulated_candidates AS
WITH source_periods AS (
  -- Update source periods if necessary
  SELECT *
  FROM fuzzy_periods
  WHERE provenance = 'S'
),
tgd AS (
  -- Update target if necessary
  SELECT * FROM target_distribution
),
candidate_distribution AS (
  SELECT admission_age, duration_group, COUNT(*) AS density
  FROM source_periods
  GROUP BY admission_age, duration_group
),
reject_proportions AS (
  SELECT t.admission_age, t.duration_group, t.density / c.density AS reject_ratio
  FROM tgd t
  LEFT JOIN candidate_distribution c
  ON c.admission_age = t.admission_age
  AND c.duration_group = t.duration_group
)
SELECT fp.*, rp.reject_ratio
FROM source_periods fp
INNER JOIN reject_proportions rp
ON fp.admission_age = rp.admission_age
AND fp.duration_group = rp.duration_group
WHERE random() <= 0.25;")

dbExecute(db, "SELECT *
  FROM simulated_candidates
  WHERE admission_age = 17;")

dbExecute(db, "INSERT INTO simulated_candidates
SELECT *, 1 AS reject_ratio
  FROM fuzzy_periods
  WHERE provenance = 'S'
  AND admission_age = 17;")

staged <- dbGetQuery(db, "SELECT * FROM projected_candidates");
write.csv(staged, file = projected_output_csv, row.names = FALSE)

staged <- dbGetQuery(db, "SELECT * FROM simulated_candidates");
write.csv(staged, file = simulated_output_csv, row.names = FALSE)

staged <- dbGetQuery(db, "SELECT provenance, id, sample_index, admission_age, admission_age_days, duration, episodes_edn
FROM periods
WHERE provenance = 'S'
AND aged_out = 'true'
AND admission_age_days + duration > (17 * 365)
UNION
SELECT provenance, id, sample_index, admission_age, admission_age_days, duration, episodes_edn
FROM fuzzy_periods
WHERE provenance = 'S' AND admission_age = 17")
write.csv(staged, file = simulated_age_out_output_csv, row.names = FALSE)

staged <- dbGetQuery(db, "SELECT provenance, id, sample_index, admission_age, admission_age_days, duration, episodes_edn
FROM periods
WHERE provenance = 'P'
AND aged_out = 'true'
AND admission_age_days + duration > (17 * 365)")
write.csv(staged, file = projected_age_out_output_csv, row.names = FALSE)

dbExecute(db, "SELECT provenance, id, sample_index, admission_age, admission_age_days, duration, episodes_edn
FROM fuzzy_periods
WHERE provenance = 'S' AND admission_age = 17")
