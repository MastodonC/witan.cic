# Duration scenario modelling

## Context

We require a means of adjusting the expected duration children will remain in care to support duration-affecting alternative scenarios.
The expected duration in care is currently calculated using survival analysis faceted by the age of admission, incorporating the probability that children will age out at the age of 18.
The distribution inferred by these processes is used as the target distribution for calculating rejection thresholds used in rejection sampling.

An alternative scenario for duration modelling could be expressed as a maximum duration that children in particular placements remain in care,
coupled with a probability that this maximum duration is applied.
If the maximum duration isn't applied, then the aforementioned target duration distribution would apply instead.

## Decision

Without clear requirements for duration scenario modelling, we propose to implement the above approach and gather feedback from customers.
The advantage of this approach is that it can be applied after the model has run, on the output CSV alone.
Conceptually, we simply need to map over periods in the output CSV and truncate them to their corresponding maximum duration, based on their placement, with a certain probability.

We will create a separate project to express this logic since this strategy doesn't require any change to the model.
It is expected the project will define a function which accepts the output CSV from the model and an input file describing the alternative duration scenario in the following format:


| placement | duration-cap-days | probability-cap-applies |
|-----------|-------------------|-------------------------|
| Q1        | 750               | 0.5                     |

The function will also need to know the projection start date to ensure that historically observed pathways aren't changed.

The alternative duration scenario will be executed and the results saved in a new output CSV with the same format as the input.

## Status

Accepted.

## Consequences

This simple approach to alternative scenario modelling will create in effect a bimodal distribution for each placement:
those for whom the duration cap is applied, and everyone else.
It will not permit us to describe mode complicated dynamics such as, for example, a tendency of those for whom the cap doesn't apply to remain in care until 18.

By definition the duration cap can't lengthen a child's time in care, only shorten it.
All alternative duration scenarios will therefore result in the same of fewer children in care, never more.






