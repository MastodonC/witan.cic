# Maximum age in days

## Context

People must leave care by their eighteenth birthday. Previously 6565 days was used as a proxy for someone being 18 years old. Anyone older than 6565 days was removed from the analysis and an NA was inserted into the output.  This lead to records being omitted from the rejection sampling.

## Decision

18 years corresponds to 6570 days (18 * 365) not including leap days, however due to not knowing precise date of births and leap years the maximum age someone can be in care (18 years) in days is 6574 or 6575 (depending on the timing of birth relative to leap years). We replaced 6565 days with 6575 and modified the predicate to remove values greater than 6575 days.

## Status

Accepted

## Consequences

### Records older than 18 years

Technically we could be including some individuals older than eighteen years, but only by a few days and the majority of analysis we do is in weeks, so should negate any obvious effects.

### Changes to previous results

This may prove to be a somewhat significant change to the model as a greater number of records will now contribute to "aging out" leavers.

### Year 1900 or 2100 problem

There is one less leap year around 1900 or 2100 as they are not a leap year because they are not divisible by 400 the way 2000 is.
