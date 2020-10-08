library(dplyr)
library(lubridate)
library(reshape2)

args = commandArgs(trailingOnly=TRUE)

input <- args[1]
output <- args[2]
project_from <- as.Date(args[3])
algorithm <- args[4]
feature_tiers <- as.integer(args[5])
seed.long <- as.integer(args[6])
set.seed(seed.long)

# input <- "/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file7431607053732494348.csv"
# output <- "/var/folders/tw/tzkjfqd11md5hdjcyjxgmf740000gn/T/file5499741138263074201.csv"
# project_from <- as.Date("2020-01-24")
# algorithm <- "euclidean_scaled"
# seed.long <- 1416938423
# set.seed(seed.long)
# feature_tiers <- 3

day_diff <- function(start, stop) {
  as.numeric(difftime(stop, start, units = "days"))
}

cluster_euclidean <- function(open.features, closed.features, scaled = TRUE) {
  features <- rbind(open.features %>% mutate(case = "Open"),
                    closed.features %>% mutate(case = "Closed"))
  
  features.txt <- features %>% select(current, case)
  features.num <- sapply(features %>% select(-current, -case), as.numeric)
  if (scaled) {
    features.num <- scale(features.num)
  }
  
  M <- features.num[features.txt$case == "Closed",]
  X <- features.num[features.txt$case == "Open",]
  
  res <- data.frame(open = c(), closed = c(), k = c(), placement = c())
  for (i in 1:nrow(X)) {
    v <- X[i,]
    open_id <- rownames(open.features[i,])
    placement <- open.features[i,]$current
    
    m <- M[closed.features$current == placement,]
    Mx <- as.matrix(m)
    # d <- ( Mx %*% v ) / sqrt( sum(v*v) * rowSums(Mx*Mx)) # Cosine
    d <- rowSums((Mx - v) ^ 2) # Euclidean
    
    m <- length(d)
    names(d) <- rownames(closed.features %>% filter(closed.features$current == placement))
    if (m > 0) {
      d <- sort(d, decreasing = FALSE)
      n <- names(d)[1:min(m,100)]
      res <- rbind(res, data.frame(open = rep(open_id, length(n)),
                                   closed = as.vector(n),
                                   k = 1:length(n),
                                   placement = placement))
    }
  }
  res
}

cluster_cosine_scaled <- function(open.features, closed.features) {
  features <- rbind(open.features %>% mutate(case = "Open"),
                    closed.features %>% mutate(case = "Closed"))
  
  features.txt <- features %>% select(current, case)
  features.num <- sapply(features %>% select(-current, -case), as.numeric)
  features.num <- scale(features.num) # Remove this for no scale
  
  M <- features.num[features.txt$case == "Closed",]
  X <- features.num[features.txt$case == "Open",]
  
  res <- data.frame(open = c(), closed = c(), k = c(), placement = c())
  for (i in 1:nrow(X)) {
    v <- X[i,]
    open_id <- rownames(open.features[i,])
    placement <- open.features[i,]$current
    
    m <- M[closed.features$current == placement,]
    Mx <- as.matrix(m)
    d <- ( Mx %*% v ) / sqrt( sum(v*v) * rowSums(Mx*Mx)) # Cosine
    
    m <- length(d)
    names(d) <- rownames(closed.features %>% filter(closed.features$current == placement))
    if (m > 0) {
      d <- sort(d, decreasing = FALSE)
      n <- names(d)[1:min(m,100)]
      res <- rbind(res, data.frame(open = rep(open_id, length(n)),
                                   closed = as.vector(n),
                                   k = 1:length(n),
                                   placement = placement))
    }
  }
  
  res
}

cluster_cosine_scaled_reverse <- function(open.features, closed.features) {
  features <- rbind(open.features %>% mutate(case = "Open"),
                    closed.features %>% mutate(case = "Closed"))
  
  features.txt <- features %>% select(current, case)
  features.num <- sapply(features %>% select(-current, -case), as.numeric)
  features.num <- scale(features.num) # Remove this for no scale
  
  M <- features.num[features.txt$case == "Closed",]
  X <- features.num[features.txt$case == "Open",]
  
  res <- data.frame(open = c(), closed = c(), k = c(), placement = c())
  for (i in 1:nrow(X)) {
    v <- X[i,]
    open_id <- rownames(open.features[i,])
    placement <- open.features[i,]$current
    
    m <- M[closed.features$current == placement,]
    Mx <- as.matrix(m)
    d <- ( Mx %*% v ) / sqrt( sum(v*v) * rowSums(Mx*Mx)) # Cosine
    
    m <- length(d)
    names(d) <- rownames(closed.features %>% filter(closed.features$current == placement))
    if (m > 0) {
      d <- sort(d, decreasing = TRUE)
      n <- names(d)[1:min(m,100)]
      res <- rbind(res, data.frame(open = rep(open_id, length(n)),
                                   closed = as.vector(n),
                                   k = 1:length(n),
                                   placement = placement))
    }
  }
  res
}

cluster_cosine_unscaled <- function(open.features, closed.features) {
  features <- rbind(open.features %>% mutate(case = "Open"),
                    closed.features %>% mutate(case = "Closed"))
  
  features.txt <- features %>% select(current, case)
  features.num <- sapply(features %>% select(-current, -case), as.numeric)
  
  M <- features.num[features.txt$case == "Closed",]
  X <- features.num[features.txt$case == "Open",]
  
  res <- data.frame(open = c(), closed = c(), k = c(), placement = c())
  for (i in 1:nrow(X)) {
    v <- X[i,]
    open_id <- rownames(open.features[i,])
    placement <- open.features[i,]$current
    
    m <- M[closed.features$current == placement,]
    Mx <- as.matrix(m)
    d <- ( Mx %*% v ) / sqrt( sum(v*v) * rowSums(Mx*Mx)) # Cosine
    
    m <- length(d)
    names(d) <- rownames(closed.features %>% filter(closed.features$current == placement))
    if (m > 0) {
      d <- sort(d, decreasing = FALSE)
      n <- names(d)[1:min(m,100)]
      res <- rbind(res, data.frame(open = rep(open_id, length(n)),
                                   closed = as.vector(n),
                                   k = 1:length(n),
                                   placement = placement))
    }
  }
  
  res
}


learner.features.closed <- function(closed_episodes, feature_tiers) {
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
    mutate(episode_days = if_else(feature_date >= report_date & feature_date <= ceased,
                                  day_diff(report_date, feature_date),
                                  day_diff(report_date, ceased)))
  
  features <- feature_episodes %>%
    group_by(period_id, day_offset) %>%
    arrange(report_date) %>%
    summarise(age = day_diff(birthday[1], feature_date[1]),
              entry = day_diff(birthday[1], beginning[1]),
              care_days = day_diff(beginning[1], feature_date[1]),
              current = last(placement),
              period_duration = last(period_duration)) %>%
      reshape2::melt(id.vars = c("period_id", "day_offset"))
  
  if (feature_tiers < 1) {
    features <- features %>% filter(variable %in% c("current", "entry", "care_days"))
  }
  
  if (feature_tiers > 1) {
    features_2 <- feature_episodes %>%
      group_by(period_id, day_offset, placement) %>%
      summarise(value = last(episode_days)) %>%
      dplyr::rename(variable = placement)
    features <- rbind(features, features_2)
  }
  
  if (feature_tiers > 2) {
    features_3 <- feature_episodes %>%
      group_by(period_id) %>%
      mutate(phase_number = cumsum(!duplicated(placement)),
             placement_seq = paste0(placement, phase_number)) %>%
      group_by(period_id, day_offset, placement_seq) %>%
      summarise(value = last(episode_days)) %>%
      dplyr::rename(variable = placement_seq)
    features <- rbind(features, features_3)
  }
  
  features <- dcast(features, period_id + day_offset ~ variable, value.var = 'value', fill = 0)
  ids <- (features[,1:2] %>% mutate(name = paste0(period_id, ':', day_offset)))$name
  features <- features[c(-2,-1)]
  rownames(features) <- ids
  features
}

learner.features.open <- function(open_episodes, feature_date, feature_tiers) {
  open_episodes <- episodes %>% filter(open)
  feature_date <- as.Date("2020-03-30")
  feature_episodes <- open_episodes %>%
    mutate(episode_days = day_diff(report_date, coalesce(ceased, feature_date)))
  
  features <- feature_episodes %>%
    group_by(period_id) %>%
    summarise(age = day_diff(birthday[1], feature_date),
              entry = day_diff(birthday[1], beginning[1]),
              care_days = day_diff(beginning[1], feature_date),
              current = last(placement)) %>%
    melt(id.vars = c("period_id"))
  
  if (feature_tiers < 1) {
    features <- features %>% filter(variable %in% c("current", "entry", "care_days"))
  }
  
  if (feature_tiers > 1) {
    features_2 <- feature_episodes %>%
      group_by(period_id, placement) %>%
      summarise(value = last(episode_days)) %>%
      dplyr::rename(variable = placement)
    features <- rbind(features, features_2)
  }

  if (feature_tiers > 2) {
    features_3 <- feature_episodes %>%
      group_by(period_id) %>%
      mutate(phase_number = cumsum(!duplicated(placement)),
             placement_seq = paste0(placement, phase_number)) %>%
      group_by(period_id, placement_seq) %>%
      summarise(value = last(episode_days)) %>%
      dplyr::rename(variable = placement_seq)
    features <- rbind(features, features_3)
  }

  features <- dcast(features, period_id ~ variable, value.var = 'value', fill = 0)
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

cluster_cases <- function(episodes, project_from, feature_tiers) {
  closed.features <- learner.features.closed(episodes %>% filter(!open), feature_tiers)
  open.features <- learner.features.open(episodes %>% filter(open), project_from, feature_tiers)
  closed.features <- add.zero.features(closed.features, open.features)
  open.features <- add.zero.features(open.features, closed.features)
  file.remove("clusters.log")
  
  clustered <- if (algorithm == "euclidean_scaled") {
    cluster_euclidean(open.features, closed.features, scaled = TRUE)
  } else if (algorithm == "euclidean_unscaled") {
    cluster_euclidean(open.features, closed.features, scaled = FALSE)
  } else if (algorithm == "cosine_scaled") {
    cluster_cosine_scaled(open.features, closed.features)
  } else if (algorithm == "cosine_unscaled") {
    cluster_cosine_unscaled(open.features, closed.features)
  } else if (algorithm == "cosine_scaled_reverse") {
    cluster_cosine_scaled_reverse(open.features, closed.features)
  }
  
  clusters <- clustered %>%
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
  mutate(ceased = if_else(is.na(lead(report_date)) & !open, end, lead(report_date))) %>%
  ungroup %>%
  as.data.frame
episodes$beginning <- ymd(episodes$beginning)
episodes$birthday <- ymd(episodes$birthday)
episodes$end <- ymd(episodes$end)
episodes$report_date <- ymd(episodes$report_date)
episodes$ceased <- ymd(episodes$ceased)
clusters <- cluster_cases(episodes, project_from, feature_tiers)
write.csv(clusters, output, row.names = FALSE)

# closed_episodes <- episodes %>% filter(!open)
# open_episodes <- episodes %>% filter(open)
