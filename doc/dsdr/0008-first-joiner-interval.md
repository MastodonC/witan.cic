# First Joiner Interval

## Context

Joiner start dates are modelled as a sequence of intervals {x1, x2, x3, ...} where each interval is sampled from an exponential distribution X.
The interval for each child joiner age is modelled by a different exponential distribution. The first projected joiner at each age should arrive interval xn after the previous joiner of that age in the empirical data.
If the previous empirical joiner of that age was some time ago, it may be highly improbable to sample an interval from X which is long enough be on or after projection start date. The new joiner must join on or after the projection start date because we can't edit the empirical history.
Simply setting the join date to be the projection start date would lead to an unlikely coincidence and a small anomalous spike in joiners.
Measuring the interval x from the projection start date would push the first joiner start date further into the future than we would expect, exaggerating the gap between joiners.

## Decision

The proposed solution is to sample an interval x (in days) from X and initiate a counter to zero.
It the sample interval plus the value of the counter results in a join date on or after the projection start date, we use the interval.
If it doesn't, we increment the counter by 1, sample another interval, and repeat.
Incrementing the counter by one each iteration guarantees that the loop will eventually terminate, so there is no other break condition.

## Status

Accepted.

## Consequences

Whilst ensuring that historical data is unchanged, this solution inherently relaxes the modelling assumptions about joiner interarrival times for the particular 'first joiner in the projection' case.
For situations where the interval must be large (compared to the average) to ensure a join date on or after the projection start date, this solution has several nice properties:
- Many samples are drawn, so the chance of drawing a large sample which bridges the gap is increased
- Each unsuccessful sample causes the counter to increase by just 1 day, ensuring that subsequent samples are modestly biased towards being accepted
- The loop terminates on the first valid sample, so on average no more bias is introduced than necessary
