![logo](https://assets.gitlab-static.net/uploads/-/system/group/avatar/10553166/logo_white.ico?width=64)

# Rust threading library

Features:
- Scoped thread pool.
- Unscoped thread pool.
- Thread pool with dynamic scheduler.
- Tasks can return output.
- Tasks can share immutable context (scoped thread pool).
- Tasks are given their dispatch index when called.
- Dispatch index is always guaranteed to match the order of calls to the dispatch function.
- Atomic cells
