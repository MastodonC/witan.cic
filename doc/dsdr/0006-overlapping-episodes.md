# Overlapping Episodes

## Context

There are often overlapping episodes as we merge the SSDA903 reports
from one year to the next

## Decision

If there are 2 overlapping episodes in a phase, take the minimum
report_date and the maximum ceased_date to turn the overlapping
episodes into one longer episode, with the placement, legal_status,
care_status, DOB and period_id from the 1st of the overlapping
episodes

## Status

Accepted.

## Consequences

It assumes that both records are correct and that the right approach
is to merge them into one rather than drop one or the other or keep
them as two episodes that run consecutively. This will affect how the
system calculates pathways between placements.
