# witan.cic
Modelling children in care demand and cost

## Usage

Refer to the template config file at data/demo/config.edn. The config file specifies file inputs, output parameters and projection parameters.

There are eight standard input files, which are created by generating candidates and rejection sampling. Rejection sampling generates a variety of input period candidates together with their rejection sampling probabilities (rejection sampling is used to ensure that overall period durations are as historically observed).

The process is as follows:

1. Execute `clj -m cic.cic run-generate-candidates-workflow` with a config to generate a period candidates file.
2. Execute `clj -m cic.cic run-rejection-sampling` with a config to generate model input files from the candidates file.
3. Execute `clj -m cic.cic run-cic-workflow` with a config to generate the projection. Depending on the config either or both of the episodes or summary csvs will be generated.

Alternatively:

1. Execute `clj -m cic.main -main` optionally passing `--config FILE` to use custom config.

## Dictionary

- Total time in care - the total length of time in care
- Period - the length of contiguous time in care
- Phase - the length of time in a placement type
- Episode - the length of time spent in the same placement/placement provider/legal status/CIN
- CIN - acronym for 'child in need', however corresponds to category of need, being the main reason a child is being provided with care services.

### Provenance tags

The episodes output contains a provenance field which contains a single character representing the source of the period.

- *H: Historic* - historic joiners who leave before the projection start date
- *O: Open* - historic joiners who have not left by the projection start date
- *P: Projected closed* - Open periods the model closes
- *C: Candidate* - potential simulated joiners whose placements and age of entry is known but who do not have a start date
- *S: Simulated* - joiners the model creates by assigning a start date

The provenance is the same for all episodes in a period.
