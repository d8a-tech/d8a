### What to look for

- **Unnecessary complexity** — over-engineering, premature abstraction, unnecessary patterns, convoluted logic
- **Logic errors** — off-by-one, wrong condition, missing case, inverted boolean
- **Unhandled errors** — swallowed, unchecked, missing early return, panic-prone paths
- **Readability** - the code should be readable top-down, try to ephasize interfaces, important logic, place helpers and util funcs below
- **Lazy nolints** — some are acceptable (tests, false positives, known tool limitations), but many are just hiding problems. Before flagging a nolint, verify that the underlying lint finding is real. If the tool is wrong, the nolint is correct and should not be treated as a review issue
- **Missing tests** — new branches or logic with no test coverage (unless tests are part of a separate tasks)
- **Broken interfaces** — signature changes, unexpected side effects, callers not updated
- **Unsafe assumptions** — nil/null dereference, race conditions, unchecked casts
- **Unnecessary exports** — types or functions exported with no justification (this is important one)
- **Test-driven degradation** — production code mutated solely to make tests possible. Clean, idiomatic production code is non-negotiable; if something is hard to test, the design should improve (better interfaces, DI at construction, smaller functions), not get worse. This is a high-severity issue.
- **Code diverging from repository conventions** — style, patterns, error handling, test structure
- **Bad comments** — comments that restate what the code does instead of explaining *why*; comments referencing code that changes independently, comments that adress the implementation process, and are just garbage during the production phase (like "    // Additional cases to exceed minimum test count and cover mixed event ordering." -> what minimum count? Possibly agent added it cause its instruction forced him to do so)
- **Dead code** — functions, types, or variables left unreachable by this change.

