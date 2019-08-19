# Remove Stale Rows

## Context

The raw episodes data may contain multiple open episodes for a child, one per report year that the episode was open.

## Decision

We want to remove open episodes from prior report years, since these are stale data.

## Status

Accepted.

## Consequences

We remove episodes without a cease date from report years prior to the latest.
