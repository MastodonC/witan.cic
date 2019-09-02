# Duration in care inference

## Context

In order to project likely future durations in care we need to summarise historic durations in care. Age of admission influences a child's duration in care, but duration is not normally distributed. Many episodes are still "open" but need to be included inthe summary duration in order to accurately represent all historic episodes.

## Decision

Survival analysis of duration in care by age of admission can infer likely distributions of durations in care whilst taking into account open episodes. This is necessary as ceased episodes will tend on average to be shorter durations, whilst currently-open episodes will tend on average to be longer durations. Survival analysis attempts to account for this discrepancy with censoring.

## Status

Accepted

## Consequences

Survival analysis compensates for the fact that many episodes are still open, but does not factor in any temporal trends to duration in care.
