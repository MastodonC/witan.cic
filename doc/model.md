# Projection the Children in Care Population


## Data

Using the SSDA903 following data is extracted for each episode, across multiple calendar years.

* Sex
* Category of need
* Legal status
* Year of birth
* Episode report date
* Episode report year (financial)
* Episode ceased date
* Placement
* Unique psuedominised ID


## Psuedominised data

To ensure protection of sensitive personal data an ID lookup is used by the Local Authority, so that whilst the model can match different episodes to an individual there is no additional personal data to enable reidentification.

Whilst individuals need to be aged, date of birth was considered too sensitive, so year of birth is used to roughly assign ages.


## Estimating duration in care by age joined

The amount of time a child is in care is influenced by the age they were when they entered care. This is driven by both younger children leaving care sooner and by 18 being the age when a child must leave care. Children five and over are more likely to stay in care longer, whilst over 10s then show a decreasing duration in care influenced by the upper age limit.

Duration in care can be summarised simply by age of admission, but this does not take into account whether episodes are still open or ceased. Episodes still open, and thus more recent, cn exhibit different trends in duration from ceased episodes. Hence it is not reasonable to either exclude one or other of these episode types, or generalise across them.

Survival analysis of duration in care by age of admission can infer likely distributions of durations in care whilst taking into account open episodes with censoring.
