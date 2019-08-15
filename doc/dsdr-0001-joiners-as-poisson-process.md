# Title

Joiners modelled as a Poisson process

# Context

Each combination of age/CIN/legal status has a mean days in between
each child joining CiC. This rate does not seem to be directly related
to the general population outside of CiC.

# Decision

Because of the lack of link to the non-CiC population, we've decided
to model joiners as a Poisson process rather than a rate on the
general population for each age as is done in witan.send.

# Status

Accepted

# Consequences

## Modelling increases in CiC due to population increase is difficult

We are choosing this route as we do *not* currently see a link between
general population and rate at which people join. If we did decide to
try to model this we could reduce the mean days between arrivals for
particular ages, but that would be a rough and indirect method.
