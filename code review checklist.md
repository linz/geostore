# Code review checklist

This document is meant to give general hints for code reviewers. It should not be considered a complete set of things to consider, and should not include anything which is currently being validated automatically.

## Testing

- Make sure the test name says what the code should do, not what it should *not* do.

   Bad example: `should not [action] when [state]`.

   Rationale: There are an infinite number of ways not to do the action. Should it skip the action, throw an exception, change the state or it put the action into a queue for reprocessing? Stating positively what the code should do makes it easier to compare the test name to its implementation to judge whether the action is appropriate and that the name is accurate.

- Use precise action wording.

   Precise examples:

   - `should return HTTP 200 when creating item`
   - `should log success message when creating item`
   - `should return ID when creating item`

   Vague examples:

   - `should fail when [state]` is not helpful, because there are an infinite number of ways in which code could "fail". A better name says something about how the code *handles* the failure, such as `should return error message when …` or `should throw FooError when …`.
  - `should succeed when [state]` has the same issue, even though there are typically only a few application-specific (for example, HTTP 200 response) or language-specific (for example, returning without throwing an exception) ways the code could reasonably be said to "succeed". This also often ends up hiding the fact that more than one thing indicates success, and each of them should probably be tested in isolation (for example, the precise examples above, each of which are side effects of the same action).

   Rationale: Precise names help with verifying that the test does what it says it should do, makes it easier to search through the tests for similar ones to use as a template for a new one, and makes it faster to understand what's gone wrong when an old test fails in CI.
