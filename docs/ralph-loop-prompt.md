# Ralph Loop Prompt for duckdb-zarr

## Task Prompt

You are working on duckdb-zarr, a DuckDB extension for querying Zarr arrays with SQL. This is part of the xarray-sql vision for making n-dimensional scientific data queryable like a database.

### Core Architecture Insights (from DESIGN.md)
- **Pivot algorithm**: Transform multidimensional arrays into tabular form using strided memory access for efficient out-of-cache processing
- **Native C++ implementation**: No external crates (like zarr-datafusion) - build minimal, focused implementation using nlohmann/json, c-blosc2, Arrow C++
- **Table functions**: Use DuckDB's table function API for `read_zarr(path)` and `read_zarr_metadata(path)`

### Your Mission
Use crosslink to track issues and work through them systematically. Check the crosslink database for high-priority items.

### Version Control Workflow
- Use **git worktrees** to manage stacked PRs - create a new worktree for each feature branch so you can work on multiple PRs simultaneously
- Rebase as necessary to keep history clean
- Commit early and often, merge to main when ready for alxmrs's review
- Feature branches should be named descriptively (e.g., `feature/zarr-metadata-parser`, `fix/ci-format`)

### Code Review Model
- All code changes require alxmrs's review, but his review does NOT block development
- Push feature branches freely and address review feedback in follow-up PRs or commits
- Use PRs to track review status but don't wait for approval before continuing

### Progress Tracking (IMPORTANT)
After completing each crosslink issue, write a progress update to `ralph-loop-progress.json` in the repo root:

```json
{
  "last_completed_issue": "<issue number and title>",
  "completed_issues": ["#18: ...", "#19: ..."],
  "current_work": "<what you're working on now>",
  "next_up": "<next high-priority issue>",
  " blockers": []
}
```

Update this file after each issue completion so Hobbes (your human collaborator) can monitor your progress.

### Stop Conditions
Continue working until you hit:
- A **blocker** that requires human input (ambiguous requirements, design decisions, external dependencies)
- An **explicit instruction to pause** from alxmrs
- When there are no more high-priority issues to work on

Do NOT stop after completing each PR or issue - keep going until you hit a stop condition.

---

## Completion Promise

```
DONE when:
- alxmrs explicitly tells you to stop
- You encounter a blocker that requires human input to proceed
- There are no more high-priority crosslink issues remaining

NOT DONE when:
- You've completed a single issue or PR
- You've hit a minor obstacle you can work around
- You're waiting for review feedback (continue with other work)
```

---

## Initial Context

Start by:
1. Reading DESIGN.md to understand the project architecture
2. Checking crosslink issues to find the highest priority item
3. Begin working on that issue
4. Update ralph-loop-progress.json after each issue completion
