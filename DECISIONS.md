# DECISIONS.md

1. **Indexes.** A unique index on `employees.emp_code` prevents duplicate identities. A unique compound index on `(emp_code, date)` is the concurrency guard for punch-in and the natural attendance key. Employee department/join-date indexes support headcount and trend queries. Attendance indexes cover employee/date lookups, date-descending list pagination, and month/date analytics. I avoided an index on every individual field because each extra index costs storage and slows writes.

2. **Punch-in race.** Both requests can pass the initial employee lookup, but they cannot both insert the same `(emp_code, date)`: MongoDB's unique index accepts one insert and rejects the other with a duplicate-key error, which the API maps to 409.

3. **Ties.** The leaderboard uses MongoDB `$rank`, not array position. Tied employees share a rank, later ranks are skipped, and the limit is applied to rank, so a tie at the cutoff is returned in full. Rows are then ordered by minutes and employee code.

4. **Headcount.** Department summaries start from employees who joined by month end and use a lookup that preserves employees with no matching attendance logs.

5. **100x data.** I would validate plans with production-sized data, add measured compound indexes, and consider pre-aggregated daily summaries if real query latency required them.
