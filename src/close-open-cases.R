library(dplyr)
library(lubridate)
library(reshape2)

args = commandArgs(trailingOnly=TRUE)

input <- args[1]
output <- args[2]
project_from <- as.Date(args[3])
seed.long <- args[4]
set.seed(seed.long)

# input <- "/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file6894966216222891812.csv"
# output <- "/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file3575231491298047884.csv"
# project_from <- as.Date("2019-03-30")
# seed.long <- 1416938423
# set.seed(seed.long)

day_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days"))
}

learner.features.closed <- function(closed_episodes) {
  # We want to create a feature vector for each month of a closed case
  # We express the offset in days using an interval of 28
  # We start 1 month after their first report date, and keep going until their last cease date
  # We also add a feature at the beginning of each placement
  # We include their age in days
  # Their time in care in days
  # Their total time in each placement in days
  # Their total time in each ordered placement in sequence in days
  
  feature_episodes <- closed_episodes %>%
    group_by(period_id) %>%
    mutate(period_duration = day_diff(min(report_date), max(ceased))) %>% ungroup %>%
    inner_join(data.frame(day_offset = c(1,2,4,6,8,10,12,14,16,18,20,24, seq(28, 18 * 365, by = 14))),by = character(0)) %>%
    mutate(feature_date = beginning + days(day_offset)) %>%
    filter(report_date < feature_date & feature_date < end) %>%
    mutate(episode_days = ifelse(feature_date >= report_date & feature_date <= ceased,
                                 day_diff(report_date, feature_date),
                                 day_diff(report_date, ceased)))
  
  features_1 <- feature_episodes %>%
    group_by(period_id, day_offset) %>%
    arrange(report_date) %>%
    summarise(age = day_diff(birthday[1], feature_date[1]),
              entry = day_diff(birthday[1], beginning[1]),
              care_days = day_diff(beginning[1], feature_date[1]),
              current = last(placement),
              period_duration = last(period_duration)) %>%
    reshape2::melt(id.vars = c("period_id", "day_offset"))
  features_2 <- feature_episodes %>%
    group_by(period_id, day_offset, placement) %>%
    summarise(value = sum(episode_days)) %>%
    dplyr::rename(variable = placement)
  
  # features_3 <- feature_episodes %>%
  #   mutate(placement_seq = paste0(placement, phase_number)) %>%
  #   group_by(period_id, day_offset, placement_seq) %>%
  #   summarise(value = sum(episode_days)) %>%
  #   dplyr::rename(variable = placement_seq)
  
  features <- rbind(features_1,
                    features_2
  ) %>%
    dcast(period_id + day_offset ~ variable, value.var = 'value', fill = 0)
  
  ids <- (features[,1:2] %>% mutate(name = paste0(period_id, ':', day_offset)))$name
  features <- features[c(-2,-1)]
  rownames(features) <- ids
  features
}

learner.features.open <- function(open_episodes, feature_date) {
  feature_episodes <- open_episodes %>%
    mutate(episode_days = day_diff(report_date, coalesce(ceased, feature_date)))
  
  features_1 <- feature_episodes %>%
    group_by(period_id) %>%
    summarise(age = day_diff(birthday[1], feature_date),
              entry = day_diff(birthday[1], beginning[1]),
              care_days = day_diff(beginning[1], feature_date),
              current = last(placement)) %>%
    melt(id.vars = c("period_id"))
  
  features_2 <- feature_episodes %>%
    group_by(period_id, placement) %>%
    summarise(value = sum(episode_days)) %>%
    dplyr::rename(variable = placement)
  
  # features_3 <- feature_episodes %>%
  #   mutate(placement_seq = paste0(placement, phase_number)) %>%
  #   group_by(period_id, placement_seq) %>%
  #   summarise(value = sum(episode_days)) %>%
  #   dplyr::rename(variable = placement_seq)
  
  features <- rbind(features_1,
                    features_2
  ) %>%
    dcast(period_id ~ variable, value.var = 'value', fill = 0)
  
  ids <- features[[1]]
  features <- features[c(-1)]
  rownames(features) <- ids
  features[is.na(features)] <- 0
  features
}

add.zero.features <- function(target, reference) {
  all_cols <- sort(union(colnames(reference), colnames(target)))
  new_cols <- setdiff(all_cols, colnames(target))
  target[new_cols] <- 0
  target <- target[, all_cols]
  target
}

is.nan.data.frame <- function(x)do.call(cbind, lapply(x, is.nan))

normalise_cols <- function(df, denominator) {
  res <- as.data.frame(sweep(df, 2, sapply(denominator, max, na.rm = TRUE), FUN = "/"))
  res[is.nan(res)] <- 0
  res
}

cluster_cases <- function(episodes, project_from) {
  closed.features <- learner.features.closed(episodes %>% filter(!open))
  open.features <- learner.features.open(episodes %>% filter(open), project_from)
  closed.features <- add.zero.features(closed.features, open.features)
  open.features <- add.zero.features(open.features, closed.features)
  
  stddev <- apply(rbind(closed.features, open.features), 2, function(x) sd(as.numeric(x)))
  means <- apply(rbind(closed.features, open.features), 2, function(x) mean(as.numeric(x)))
  
  M <- closed.features[,c("current", "period_duration", "care_days", "entry")]
  X <- open.features[1:nrow(open.features),c("current", "care_days", "entry")]
  X$entry <- (as.numeric(X$entry) - means["entry"]) / stddev["entry"]
  X$care_days <- (as.numeric(X$care_days) - means["care_days"]) / stddev["care_days"]
  M$entry <- (as.numeric(M$entry) - means["entry"]) / stddev["entry"]
  M$care_days <- (as.numeric(M$care_days) - means["care_days"]) / stddev["care_days"]
  
  res <- data.frame(open = c(), closed = c(), k = c(), placement = c())
  file.remove("clusters.log")
  for (i in 1:nrow(X)) {
    v <- X[i,]
    placement <- v[["current"]]
    vxx <- as.numeric(v[!v == placement])
    vx <- vxx
    Mxx <- M[M$current == placement & as.numeric(M$period_duration) > as.numeric(v["care_days"]), !(names(M) %in% c("current", "period_duration"))]
    Mx <- Mxx
    Mx[] <- lapply(Mxx, as.numeric)
    Mx <- as.matrix(Mx)
    # sim <- ( Mx %*% vx ) / sqrt( sum(vx*vx) * rowSums(Mx*Mx) )
    # names(sim[which.max(sim),])
    d <- rowSums((Mx - vx) ^ 2)
    m <- length(d)
    if (m > 0) {
      d <- sort(d)
      n <- names(d)[1:min(m,100)]
      res <- rbind(res, data.frame(open = rep(rownames(v), length(n)), closed = as.vector(n), k = 1:length(n), placement = placement))
    }
  }
  clusters <- res %>%
    mutate(offset =  as.numeric(sub(".*:", "", closed)),
           closed = sub(":.*", "", closed))
  dedup <- clusters[!duplicated(clusters[,c("closed", "open")]),] %>%
    group_by(open) %>%
    top_n(10, -k) %>%
    ungroup
  write.table(dedup %>% mutate(seed = seed.long), "clusters.log", sep = ",", col.names = !file.exists("clusters.log"), append = T)
  dedup
}

episodes <- read.csv(input, na.strings = "") %>%
  mutate(open = open == "true") %>%
  group_by(period_id) %>%
  arrange(period_id, report_date) %>%
  mutate(ceased = ifelse(is.na(lead(report_date)) & !open, end, lead(report_date))) %>%
  ungroup %>%
  as.data.frame
episodes$beginning <- ymd(episodes$beginning)
episodes$birthday <- ymd(episodes$birthday)
episodes$end <- ymd(episodes$end)
episodes$report_date <- ymd(episodes$report_date)
episodes$ceased <- ymd(episodes$ceased)
clusters <- cluster_cases(episodes, project_from)
write.csv(clusters, output, row.names = FALSE)
