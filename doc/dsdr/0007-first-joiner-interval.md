# First Joiner Interval

## Context

Joiner start dates are modelled as a sequence of intervals {x1, x2, x3, ...} where each interval is sampled from an exponential distribution X.
Each child joiner age is modelled by a different exponential distribution. The first projected joiner at each age should arrive interval xn after the previous joiner of that age in the empirical data.
If the previous empirical joiner of that age was some time ago, it may be highly unfeasible to sample an interval from X which is long enough be beyond the projection start date.
A previous solution couldn't find candidate intervals even after 50 samples.
However, we can't change data prior to the projection start date. Allowing those joiners to arrive on the projection start date would lead to an unlikely coincidence and a small anomalous spike in joiners.
Measuring the interval x from the projection start date would push the first joiner start date further into the future than we would expect, exacerbating the gap between joiners.

## Decision

The proposed solution is to sample intervals x from X.
After each sample, we check whether the effective start date of the new joiner is after the projection start date.
If it is, we use this interval.
If it isn't, we sample another interval and add this to the previous interval to create a larger interval.
We repeat this loop as often as necessary.

## Status

Accepted.

## Consequences

Whilst ensuring that historical data is unchanged, this solution inherently relaxes the modelling assumptions about joiner interarrival times for the particular 'first joiner in the projection' case.
It may result in a slight underestimate of joiners in the period immediately after the projection start, as for infrequent joiner ages the intervals may be large, and therefore the interval sum may step quickly beyond the projection start date.
