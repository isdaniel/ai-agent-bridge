---
description: Generate Word documents (.docx). Use when the user asks to create a document, letter, memo, proposal, or report in Word format.
allowed-tools: Bash(pip install *) Bash(python3 *)
---

Generate a Word document using Python and python-docx.

## Setup

```!
pip install python-docx -q 2>&1 | tail -1
```

## Instructions

1. Create the document with `python-docx`
2. Use proper heading hierarchy (Heading 1, 2, 3)
3. Add tables with styled headers when presenting structured data
4. Use bullet/numbered lists for enumerations
5. Save to `/tmp/<descriptive-name>.docx`
6. Output the absolute file path

## Formatting guidelines

- Title: Heading 1 at the top
- Sections: Heading 2 for major sections, Heading 3 for subsections
- Tables: bold header row, consistent column alignment
- Keep paragraphs concise and professional
