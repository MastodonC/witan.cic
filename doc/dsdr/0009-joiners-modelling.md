# Joiners modelling

## Context

When generating future projections we need a means of deciding how many children will join care and how quickly. A flexible joiners component is required: one which can estimate future joiners based on the past (for baseline projections) and which can be configured externally with target joiner rates to support scenario modelling.

## Decision

Three joiner strategies are coded: an untrended strategy, a trended strategy, and an alternate scenario strategy.

Untrended. The untrended strategy is invoked with the following configuration:
`:joiners {:model :untrended :train-date-range [#date "yyyy-mm-dd" #date "yyyy-mm-dd"]}`
This model analyses joiners who arrive within the train date rage by grouping them into chunks of 84 days. The chunks are aligned with the final date in the range, and the first (likely partial) chunk is thrown away. The model aggregates the number of joiners of a particular admission age per chunk to create a set of counts. These counts are bootstrap sampled and the mean is taken. This value is taken to be the target number of joiners per 84 days.

Trended. The trended strategy is invoked with the following configuration:
`:joiners {:model :trended :train-date-range [#date "yyyy-mm-dd" #date "yyyy-mm-dd"]}`
This model analyses joiners who arrive within the train date range by grouping them into chunks of 84 days. The chunks are aligned with the final date in the range, and the first (likely partial) chunk is thrown away. The model performs Poisson regression on the joiner counts per 84 days to produce an expectation of future joiner counts per 84 days. The model samples from the covariance matrix to preserve the uncertainty in the model. Each projection a different sample is taken, yielding different parameters for the linear model.

Scenario. The scenario strategy is invoked with the following configuration:
`:joiners {:model :scenario}`. In addition the `:scenario-joiner-rates` input file must be specified.
The senario joiner rates file has 19 columns. The first column is named `date` and it contains a date in yyyy-mm-dd format. The remaining columns are labelled 0-17 and they each contain the target joiner rate per month for that admission age.


## Status

Accepted.

## Consequences

The joiner config must be updated to ensure that the correct joiners model is used.

Joiners modelling is limited to the three strategies listed above.

