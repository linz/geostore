---
name: User story
about: Suggest an idea to give the user a more valuable product
labels: user story
---

### User Story

<!-- A user story to describe why a user wants to do something, who the user is and what they want to do -->

So that [some reason], as a [role], I want to [do something].

<!-- optional: Instead of [existing behaviour] -->

#### Acceptance Criteria

<!-- Required artifacts to accept this feature as completed. -->

- [ ] Given [a pre-condition], when [an event happens], then [an expected outcome occurs]
- [ ] Given [a pre-condition], when [an event happens], then [an expected outcome occurs]

#### Additional context

<!-- Add any other context or mocked CLI commands or screenshots about the feature request here.-->

#### Tasks

<!-- Tasks needed to complete this enabler -->

- [ ] ...
- [ ] ...

#### Definition of Ready

- [ ] This story is **ready** to work on
  - [ ] Independent (story is independent of all other tasks)
  - [ ] Negotiable (team can decide how to design and implement)
  - [ ] Valuable (from a user perspective)
  - [ ] Estimate value applied (agreed by team)
  - [ ] Small (so as to fit within an iteration)
  - [ ] Testable (in principle, even if there isn't a test for it yet)
  - [ ] Environments are ready to meet definition of done
  - [ ] Resources required to implement will be ready
  - [ ] Everyone understands and agrees with the tasks to complete the story
  - [ ] Release value (e.g. Iteration 3) applied
  - [ ] Sprint value (e.g. Aug 1 - Aug 15) applied

#### Definition of Done

- [ ] This story is **done**:
  - [ ] Acceptance criteria completed
  - [ ] Automated tests are passing
  - [ ] Code is peer reviewed and pushed to master
  - [ ] Deployed successfully to test environment
  - [ ] Checked against [CODING guidelines](https://github.com/linz/geostore/blob/master/CODING.md)
  - [ ] Relevant new tasks are added to backlog and communicated to the team
  - [ ] Important decisions recorded in the issue ticket
  - [ ] Readme/Changelog/Diagrams are updated
  - [ ] Product Owner has approved acceptance criteria as complete
  - [ ] Meets non-functional requirements:
    - [ ] Scalability (data): Can scale to 300TB of data and 100,000,000 files and ability to
          increase 10% every year
    - [ ] Scability (users): Can scale to 100 concurrent users
    - [ ] Cost: Data can be stored at < 0.5 NZD per GB per year
    - [ ] Performance: A large dataset (500 GB and 50,000 files - e.g. Akl aerial imagery) can be
          validated, imported and stored within 24 hours
    - [ ] Accessibility: Can be used from LINZ networks and the public internet
    - [ ] Availability: System available 24 hours a day and 7 days a week, this does not include
          maintenance windows < 4 hours and does not include operational support
    - [ ] Recoverability: RPO of fully imported datasets < 4 hours, RTO of a single 3 TB dataset <
          12 hours
