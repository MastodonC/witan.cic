### Applying birthdays with only a year of birth

## Context

In order to aid in preventing re-identification episode data only birth years have been provided. However in order to provide a more realistic distribution of ages and birthdays for modelling, arbitary birth dates are required.

## Decision

Each projection will use randomly generated birthdays with corresponding random ages of admission, EACH CONTROLLED WITH A SEED (not yet implemented). This allows the output to account for uncertainty in the input.

## Status

Accepted.

## Consequences

Actual counts of individuals at particular ages are simply approximations and only generally reflect the "real" population. This methodology does not capture any trends that may be present in age distribution, for example if more children are born in August.
