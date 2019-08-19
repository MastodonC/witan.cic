# Summarise Periods

## Context

We need to summarise additional information about each period for modelling.

## Decision

Calculate whether a period is still open, how long it is/was and how many episodes it constitutes.

## Status

Accepted.

## Consequences

Three additional keys added to each period; `:open?`, `:duration` and `:episodes`.
