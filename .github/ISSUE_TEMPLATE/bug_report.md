---
name: Bug report
about: Create a report to help us improve
title: 'Bug: '
labels: 'bug'
---

<!--
Checklist before submitting:

- [ ] Search through existing issue reports to check whether the issue already exists
- [ ] If relevant, please include or link to a small sample dataset
- [ ] Provide stacktrace / debugging messages where possible
-->

## Bug Description

<!-- A clear and concise description of what the bug is. -->

## How to Reproduce

<!-- Steps, sample datasets, config and commands/or steps to reproduce the behavior. -->

1. Do …
1. Run `…`

What did you expect to happen? <!-- Describe the expected result -->

What actually happened? <!-- Describe the actual outcome -->

## Software Context

Operating system: <!-- e.g. Windows / Linux / macOS -->

Environment: <!-- e.g. production -->

Relevant software versions:

- AWS CLI: <!-- include the output of `aws \-\-version` -->
- Poetry: <!-- include the output of `poetry \-\-version` -->
<!-- Any other relevant software -->

## Additional context

<!-- Add any other context about the problem here, such as stack traces or debugging info. -->

#### Definition of Done

- [ ] This bug is **done**:
    - [ ] Bug resolved to **user's** satisfaction
    - [ ] Automated tests are passing
    - [ ] Code is peer reviewed and pushed to master
    - [ ] Deployed successfully to test environment
    - [ ] Checked against CODING guidelines
    - [ ] Relevant new tasks are added to backlog and communicated to the team
    - [ ] Important decisions recorded in the issue ticket
    - [ ] Sprint board is updated
    - [ ] Readme/Changelog/Diagrams are updated
    - [ ] Product Owner has approved as complete
    - [ ] Meets non-functional requirements:
        - [ ] Capacity (in total): At least 300TB of data and 100,000,000 files it total and ability to grow 10% every year and a single dataset can contain 1,000,000 files and xx GB (TODO: confirm).
        - [ ] Capacity (per dataset): Up to 1TB and 150,000 files
        - [ ] Cost: Data can be stored at < 0.5 NZD per GB per year (cost calculation
        - [ ] Performance: A large dataset (500 GB and 50,000 files - e.g. Akl aerial imagery) can be validated, imported and stored within 24 hours
        - [ ] Accessibility: Can be used from LINZ networks and the public internet
        - [ ] Availability: System available 24 hours a day and 7 days a week, this does not include maintenance windows < 4 hours and does not include operational support
        - [ ] Recoverability: RPO of fully imported datasets < 4 hours, RTO of a single 3 TB dataset < 12 hours
