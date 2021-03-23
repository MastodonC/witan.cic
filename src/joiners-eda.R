

print(mod)
params <- mvrnorm(100, coef(mod), vcov(mod))

params_age <- function(params, age) {
  if (age == 0) {
    cbind(id = 1:nrow(params),
          params[,c("(Intercept)", "quarter")],
          "admission_age" = 0,
          "quarter:admission_age" = 0) %>%
      as.data.frame %>%
      setNames(c("id", "intercept", "quarter", "admission_age", "quarter:admission_age"))
  } else {
    cbind(id = 1:nrow(params), params[,c("(Intercept)", "quarter", paste0("admission_age", age), paste0("quarter:admission_age", age))]) %>%
      as.data.frame %>%
      setNames(c("id", "intercept", "quarter", "admission_age", "quarter:admission_age"))
  }
}


params_age(params, 1)

dat %>%
  filter(admission_age == 0) %>%
  ggplot(aes(quarter, n)) + geom_point()

dates <- data.frame(date = seq(min(dat$quarter), max(dat$quarter) + years(4), "84 days"))

age <- 14

poisson <- Vectorize(function(lambda){
  rpois(1, lambda)
})

for (age in 0:1) {
  pp <- params_age(params, age)
  pps <- sample_n(pp, 1)
  print(ggplot(data = NULL) +
          geom_point(data = dat %>% filter(admission_age == age), aes(quarter, n), colour = "orange", size = 10) +
          geom_line(data = dates %>%
                      inner_join(pp, by = character(0)) %>%
                      mutate(n = exp(intercept + admission_age + as.numeric(date) * quarter + as.numeric(date) * `quarter:admission_age`)) %>%
                      dplyr::select(id, date, n) %>%
                      rename(quarter = date),
                    aes(quarter, n, group = id),
                    alpha = 0.25) +
          geom_line(data = dates %>%
                      inner_join(pps, by = character(0)) %>%
                      mutate(n = exp(intercept + admission_age + as.numeric(date) * quarter + as.numeric(date) * `quarter:admission_age`)) %>%
                      dplyr::select(id, date, n) %>%
                      rename(quarter = date),
                    aes(quarter, n, group = id),
                    colour = "purple", size = 1) +
          geom_point(data = dates %>%
                       filter(date > max(dat$quarter)) %>%
                       inner_join(pps, by = character(0)) %>%
                       mutate(n = poisson(exp(intercept + admission_age + as.numeric(date) * quarter + as.numeric(date) * `quarter:admission_age`))) %>%
                       dplyr::select(id, date, n) %>%
                       rename(quarter = date),
                     aes(quarter, n),
                     size = 10, colour = "purple", shape = 1, stroke = 1.5,) +
          labs(title = age))
}

rpois()


