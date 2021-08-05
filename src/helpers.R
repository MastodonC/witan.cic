library(ggplot2)
library(lubridate)

chart_title <- function(title){
  paste(la_label, "-", title)
}

chart_path <- function(path) {
  file.path(output_dir, "charts", paste0(Sys.Date(),"-",basename(path)))
}

year_start <- function(year) {
  as.Date(paste0(year, "-01-01"))
}

year_end <- function(year) {
  year_start(year + 1) - 1
}

month_start <- function(month) {
  as.Date(paste0(month, "-01"))
}

month_end <- function(month) {
  as.Date(paste0(month, "-01")) + months(1) - days(1)
}

years_before <- function(date, ys) {
  date - years(ys)
}

date_between <- function(start, end) {
  out <- numeric(length = length(start))
  for(i in seq_along(start)) {
    out[i] <- sample(seq(min(start[i], end[i]),
                         max(start[i], end[i]),
                         by = "day"), 1)
  }
  as.Date(out)
}


birthday_before_date <- function(birth_date, other_date) {
  yr <- year(other_date)
  a <- birth_date
  year(a) <- yr
  b <- birth_date
  year(b) <- (yr - 1)
  if (a > other_date){
    b
  } else {
    a
  }
}

imputed_birthday <- function(birth_month, min_start, max_cease) {
  earliest_possible <- max(max_cease - days(floor(18 * 365.25)) + 1, month_start(birth_month))
  latest_possible <- min(min_start, month_end(birth_month))
  date_between(earliest_possible, latest_possible)
}

year_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days")) %/% 365.25
}

quarter_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days")) %/% 91.3125
}

month_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days")) %/% 30
}

day_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days"))
}

theme_mastodon <- theme(plot.title = element_text(family = "Open Sans SemiBold",
                                                  hjust = 0.5, size = 20,
                                                  margin = margin(0,0,15,0)),
                        axis.title = element_text(family = "Open Sans SemiBold",
                                                  hjust = 0.5, size = 16),
                        axis.text = element_text(family = "Open Sans",
                                                 hjust = 0.5, size = 10),
                        axis.text.x = element_text(angle = -45),
                        axis.title.x = element_text(margin = margin(15,0,0,0)),
                        axis.title.y = element_text(margin = margin(0,10,0,0)),
                        plot.margin = margin(10,20,10,10),
                        panel.grid = element_line(color = "#FFFFFF"))

impute.quantiles <- function(df) {
  res <- df %>% as.data.frame %>% mutate(`100` = coalesce(`100`, 18:1 * 365))
  res <- t(na.approx(t(res))) %>% as.data.frame
  res <- cbind(age = str_replace(rownames(df),"admission_age=", ""), res)
  colnames(res) <- c("age", 0:100)
  res
}

assoc.period.id <- function(episodes) {
  episodes <- episodes %>% arrange(ID, report_date)
  new_periods <- coalesce(episodes$ID == lag(episodes$ID) & episodes$report_date > lag(episodes$ceased), FALSE)
  episodes$period_id <- paste0(episodes$ID, "-", ave(ifelse(new_periods, 1.0, 0.0), episodes$ID, FUN = cumsum) + 1)
  episodes
}

clamp <- Vectorize(function(x, lower, upper) {
  max(lower, min(upper, x))
})

binomial_noise <- Vectorize(function(val, max_val, noise) {
  p <- max(1.0 / max_val, min(1.0, val / max_val))
  max(rbinom(n = 1, size = max_val %/% noise, prob = p) * noise, 1)
})

impute_birthday <- Vectorize(function(lower, upper) {
  lower + days(as.integer(round(runif(1, 0, as.numeric(difftime(upper, lower, units = "days"))))))
})

eighteen_years_before <- function(date) {
  if_else(day(date)==29 & month(date) == 2,
    date + days(1) - years(18),
    date - years(18))
}

eighteen_years_after <- function(date) {
  if_else(day(date)==29 & month(date) == 2,
    date + days(1) + years(18),
    date + years(18))
}

survival_data <- function(episodes) {
  episodes %>%
    mutate(report_date = ymd(report_date),
           ceased = ymd(ceased)) %>%
    group_by(period_id) %>%
    dplyr::summarise(period_start = min(report_date),
                     period_end = max(ceased),
                     birth_month_beginning = ymd(paste0(DOB[1], "-01")),
                     birth_month_end = birth_month_beginning + months(1) - days(1))
}

survival_data2 <- function(survival_data, extract_date) {
  # We think the child aged out if they *could* have hit 18 years - 1 day given their birthday range
  # That means if we assume the oldest they could be, did that cross 18 years - 1 day.
  # That means if we take their earlier birthday, is it *at least* 18 years - 1 day.
  survival_data %>%
    mutate(birthday_lower = pmax(birth_month_beginning, eighteen_years_before(replace_na(period_end, extract_date))),
           birthday_upper = pmin(birth_month_end, period_start),
           # aged_out = year_diff(birth_month_beginning, replace_na(period_end, extract_date)) >= 18,
           # Removing this period_end assignment - don't think we need it
           # period_end = if_else(aged_out, eighteen_years_after(birthday_upper) - days(1), period_end),
           event = !is.na(period_end),
           duration = if_else(event,
                              day_diff(period_start, period_end),
                              day_diff(period_start, extract_date)))
}

# These cases violate one of more of our assumptions. We'll assume they left at 18.
# survival_data2 %>% filter(birthday_lower > birthday_upper)

survival_data3 <- function(survival_data2, noise) {
  survival_data2 %>%
    mutate(birthday_lower = pmin(birthday_lower, birthday_upper) # Only matters if there are age 18+ leavers above
    ) %>%
    inner_join(data.frame(n = 1:100), by = character()) %>%
    mutate(birthday = as.Date(impute_birthday(birthday_lower, birthday_upper), origin = "1970-01-01"),
           admission_age = as.factor(clamp(year_diff(birthday, period_start), 0, 17)),
           max_duration = day_diff(period_start, eighteen_years_after(birthday)),
           duration = pmin(duration, max_duration),
           fuzzed_duration = binomial_noise(duration, max_duration, noise)
    ) %>%
    as.data.frame
}

survival_data4 <- function(survival_data3) {
  survival_data3 %>%
    mutate(aged_out = year_diff(birthday, period_start + days(fuzzed_duration)) >= 17, # Going to say no one leaves aged 17
           event = event | aged_out, # All aged out children are leavers
           duration = if_else(aged_out, max_duration, duration),
           fuzzed_duration = if_else(aged_out, max_duration, fuzzed_duration),
           fuzzed_exit_age = fuzzed_duration + day_diff(birthday, period_start))
}

age_categories <- c("Age 0", "Age 1-5", "Age 6-10", "Age 11-15", "Age 16", "Age 17")

age_category <- function(age) {
  case_when(age == 0 ~ age_categories[1],
            age %in% 1:5 ~ age_categories[2],
            age %in% 6:10 ~ age_categories[3],
            age %in% 11:15 ~ age_categories[4],
            age == 16 ~ age_categories[5],
            age %in% 17:18 ~ age_categories[6],
            TRUE ~ "Other")
}

labelled_episodes <- function(periods, date_range) {
  data.frame(range_start = date_range) %>%
    mutate(range_end = range_start + months(1) - 1)  %>%
    inner_join(periods, by = character()) %>%
    mutate(cic_start = pmax(period_start, range_start),
           cic_end = pmin(period_end, range_end),
           age_start = year_diff(birthday, cic_start),
           age_end = year_diff(birthday, cic_end),
           age_category_joined = age_category(year_diff(birthday, period_start)),
           age_category_left = age_category(year_diff(birthday, period_end)),
           age_category_start = age_category(age_start),
           age_category_end = age_category(age_end),
           joined = period_start >= range_start & period_start <= range_end,
           left = period_end >= range_start & period_end <= range_end,
           cic = period_start <= range_end & period_end >= range_start,
           aged = cic & age_category_start != age_category_end)
}

joiners_leavers_net <- function(labelled_episodes) {
  chart_data <- rbind(
    cbind(labelled_episodes %>%
            filter(joined) %>%
            group_by(simulation, range_start, age_category_joined) %>%
            rename(age_category = age_category_joined) %>%
            summarise(n = n()),
          label = "joiners"),
    cbind(labelled_episodes %>%
            filter(left) %>%
            group_by(simulation, range_start, age_category_left) %>%
            rename(age_category = age_category_left) %>%
            summarise(n = n()),
          label = "leavers"),
    cbind(labelled_episodes %>%
            filter(aged) %>%
            group_by(simulation, range_start, age_category_end) %>%
            rename(age_category = age_category_end) %>%
            summarise(n = n()),
          label = "aged-in"),
    cbind(labelled_episodes %>%
            filter(aged) %>%
            group_by(simulation, range_start, age_category_start) %>%
            rename(age_category = age_category_start) %>%
            summarise(n = n()),
          label = "aged-out")
  ) %>%
    complete(range_start, age_category, fill = list(n = 0))
  rbind(chart_data,
        dcast(simulation + range_start + age_category ~ label, value.var = "n", data = chart_data, fill = 0) %>%
          mutate(net = joiners + `aged-in` - `aged-out` - leavers ) %>%
          dplyr::select(simulation, range_start, age_category, net) %>%
          rename(n = net) %>%
          mutate(label = "net"))
}

financial_year <- function(date) {
  y <- year(date - months(3))
  paste0(y, "/", substr(y + 1, 3, 4))
}
