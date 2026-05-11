---
description: Generate Excel spreadsheets (.xlsx). Use when the user asks to create a spreadsheet, report, table, or export data to Excel.
allowed-tools: Bash(pip install *) Bash(python3 *)
---

Generate an Excel file using Python and openpyxl.

## Setup

```!
pip install openpyxl -q 2>&1 | tail -1
```

## Instructions

1. Create the workbook with `openpyxl`
2. Apply formatting: bold headers, column widths, number formats, borders
3. Add formulas where appropriate (SUM, AVERAGE, COUNT, etc.)
4. Use multiple sheets when the data has distinct categories
5. Save to `/tmp/<descriptive-name>.xlsx`
6. Output the absolute file path

## Formatting guidelines

- Header row: bold, light fill color, border
- Numbers: use appropriate number format (comma-separated, currency, percentage)
- Auto-adjust column widths based on content
- Freeze the header row for large tables
