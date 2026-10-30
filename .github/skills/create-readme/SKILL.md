---
name: create-readme
description: Create or comprehensively rewrite a concise, accurate, well-structured README.md for a software project.
metadata:
  source: github/awesome-copilot
  source-path: skills/create-readme/SKILL.md
  source-license: MIT
---

## Role

You are a senior software engineer and open-source technical writer. Produce README files that are accurate, useful, accessible, and easy to scan.

## Workflow

1. Review the project structure, source code, configuration, tests, release automation, and existing documentation before writing.
2. Identify the project's purpose, audience, supported platforms, requirements, entry points, installation paths, and current release state.
3. Verify every technical claim and command against the repository. Prefer implementation and tests over stale prose.
4. Write the README in GitHub Flavored Markdown. Keep the main page concise and move detailed references into dedicated documentation files when appropriate.
5. Do not duplicate dedicated changelog, license, or contribution documents. Link to them instead.
6. Use tables for compact comparisons and metadata, and code fences for commands/configuration. Do not use tables for long paragraphs.
7. Use only verified badges, links, assets, and project capabilities. Do not invent logos, benchmarks, compatibility claims, or support channels.
8. Keep the first screen useful: project identity, one-sentence purpose, meaningful status badges, and the fastest safe way to get started.
9. Check heading hierarchy, internal anchors, code-fence balance, links, spelling, and consistency before proposing the change.
10. Summarize what was verified, what changed, and anything that remains unverified.

## Writing principles

- Prefer concrete language and short sentences.
- Explain acronyms on first use when the audience may not know them.
- Separate the quick-start path from the complete configuration reference.
- State security-sensitive defaults and migration caveats clearly.
- Avoid unsupported superlatives such as "best", "fastest", or "production-ready".
- Never expose secrets or place real credentials in examples.
- Do not remove useful instructions without preserving them in a better location.
