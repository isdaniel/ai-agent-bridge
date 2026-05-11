---
description: Analyze CSV or Excel data files. Use when the user provides a data file and wants summaries, statistics, pivot tables, or charts.
allowed-tools: Bash(pip install *) Bash(python3 *)
---

Analyze data files using Python with pandas.

## Setup

```!
pip install pandas openpyxl matplotlib -q 2>&1 | tail -1
```

## Instructions

1. Load the data with `pandas` (auto-detect CSV encoding and delimiter)
2. Show basic info: shape, columns, dtypes, missing values
3. Provide summary statistics for numeric columns
4. Answer the user's specific questions about the data
5. When asked, generate:
   - Pivot tables
   - Group-by aggregations
   - Charts (bar, line, pie) saved as PNG to `/tmp/`
   - Filtered/transformed Excel exports to `/tmp/`
6. Output file paths for any generated files

## Output guidelines

- Start with a brief text summary of the data
- Use tables (formatted text) for small results
- Generate Excel/chart files for large or visual results
- When the user's question is vague, show the top patterns and ask what to explore
