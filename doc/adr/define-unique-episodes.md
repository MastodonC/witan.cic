# Define unique episodes

## Context

Each child has a unique ID. Some children leave care and return again later.

## Decision

As each period in care is modelled separately, we assign each period its own ID.

## Status

Accepted.

## Consequences

An additional `period-is` is assigned to each period for each child.
