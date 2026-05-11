---
description: Translate text or documents between languages. Use when the user asks to translate content, a file, or text between any languages.
---

Translate content between languages.

## Instructions

1. Detect the source language if not specified
2. Translate to the requested target language
3. Preserve the original formatting (headings, lists, tables)
4. For document files (.docx, .txt): read the file, translate, and create a new file with the translated content at `/tmp/<name>_<target-lang>.<ext>`
5. For short text: reply directly with the translation
6. Output the file path if a file was created

## Quality guidelines

- Maintain natural phrasing in the target language (not literal word-for-word)
- Preserve technical terms, proper nouns, and brand names
- Keep the same tone (formal/informal) as the source
- If a term has no direct translation, provide the closest equivalent with a brief note
