# AGENTS.md

## Project overview
This repository contains a public health data project in Python focused on motorcycle fleet growth, trauma hospitalizations, mortality, and emergency care capacity in Brazil.

The repository starts empty and must be built incrementally.

## General coding rules
- All code, variable names, function names, comments, docstrings, commit-style messages, and file names must be in English.
- Prefer clarity over cleverness.
- Keep functions small and focused.
- Avoid hidden magic and implicit behavior.
- Use type hints whenever reasonable.
- Use pathlib instead of raw string paths when possible.
- Use logging instead of print for pipelines, except in very small CLI examples.
- Prefer reproducible scripts over notebook-only logic.

## Dependency management
- Use Poetry for dependency management.
- Keep dependencies minimal.
- Separate runtime and dev dependencies when appropriate.

## Project structure
Always preserve and extend this structure when relevant:

- `src/`
- `data/raw/`
- `data/interim/`
- `data/processed/`
- `notebooks/`
- `outputs/figures/`
- `outputs/tables/`
- `tests/`
- `.vscode/`

## Data ingestion rules
- If a dataset can be downloaded programmatically in a stable way, implement the downloader.
- If automatic download is not reliable or not possible, do not fake it.
- Instead, explicitly document:
  - which files are needed,
  - from which institution,
  - from which page/system,
  - expected filename pattern,
  - where the user must place them locally.

## Teaching-oriented output
Every substantial response must in portuguese-brazil and include:

1. What was implemented
2. Why this step matters in the project
3. Main concepts applied
4. Important code excerpts with explanation
5. How to run
6. What to validate before moving on
7. Next recommended step

Assume the user is learning data engineering, public health data pipelines, and applied statistics. Be explanatory, but still practical and concise.

## Execution and developer experience
- Keep scripts runnable from VS Code launch configurations when appropriate.
- Prefer creating reusable CLI entry points for pipelines.
- Update `.vscode/launch.json` whenever a new major runnable entry point is introduced.

## Safety and honesty
- Do not invent dataset schemas.
- If a schema is uncertain, say so clearly and isolate assumptions.
- If a file is required but unavailable, explain exactly what is needed.
- Do not pretend that a pipeline was validated if it was not.

## Expected style for deliverables
When generating code or repository changes, also explain:
- the reasoning behind the folder/file design,
- why certain abstractions were chosen,
- how the current step connects with the research question.