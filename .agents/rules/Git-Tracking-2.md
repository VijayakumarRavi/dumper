---
trigger: always_on
---

33. Version Changes Must Be Intentional

Do not bump:

tool version
repository format version
stream format version

automatically for every commit.

These are separate concepts.

The project already distinguishes tool, repository, and stream versioning; preserve that separation.

34. Review Before Finalizing

Before reporting work as complete:

git status
git diff HEAD
git log -5 --oneline

Confirm:

working tree state is understood
all intended changes are committed
no accidental files are present
tests correspond to the implementation
documentation matches the implementation
35. Final Git State

At the end of a coding task, report:

Branch:
Commits created:
Tests run:
Working tree:

If unrelated pre-existing changes remain, explicitly say so.

Do not claim the working tree is clean unless it actually is.

36. Git Commit Checklist

Before committing:

[ ] git status reviewed
[ ] intended files only
[ ] no secrets
[ ] git diff reviewed
[ ] git diff --check passes
[ ] relevant tests pass
[ ] commit message is specific
[ ] no unrelated refactoring

Before finishing:

[ ] final tests pass
[ ] final diff reviewed
[ ] commits are logically grouped
[ ] no accidental generated files
[ ] documentation matches behavior
[ ] repository status understood
37. Golden Git Rule

The Git history should tell the same story as the codebase.

A future engineer should be able to inspect:

git log
git show
git blame
git bisect

and understand:

why the change exists
what problem it solved
how it was tested
what compatibility impact it had

Do not optimize the commit history for convenience.

Optimize it for future debugging, auditing, review, rollback, and trust.