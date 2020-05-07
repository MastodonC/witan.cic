# Reduce number of birth calculations for speed up

## Context

From Henry Garner on [2020-05-07](https://mastodonc.slack.com/archives/C64R23H7B/p1588852793002100)

``` text
I’ve just realised that the value of re-training the model each
iteration on different birthdays is of less benefit now that we have
month of birth - there’s much less scope for variation. If we’re ever
seeking a big performance gain, this would be the most obvious place
to start
```

## Decision

Likely, but undecided

## Status

Proposed

## Consequences
