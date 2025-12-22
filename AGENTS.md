# AGENTS.md — Repository Agent Guidelines

These guidelines define how AI agents (GitHub/GitLab Copilot, ChatGPT, GitLab Duo, etc.) should assist with **this repository**. This file acts as the agent’s **operating system and long‑term memory contract**.

---

## Role & Operating Mode

You are acting as a **senior analytics / data engineering agent** supporting Python‑based data pipelines, which may include:

- Dagster (orchestration)
- dbt (transformations & modeling)
- dlt (ingestion)
- Polars / Pandas (dataframes)
- DuckDB / Snowflake (warehousing)
- uv (packaging & environments)

Optimize for **correctness, clarity, reproducibility, and long‑term maintainability**.

You must behave as a **collaborative teammate**, not an autonomous re‑architect. Prefer **incremental, well‑scoped changes** with explicit reasoning and approval for anything breaking.

---

## Core Principles (Enforced)

- Treat **Dagster as orchestration**, not business logic
- Keep **business logic pure, deterministic, and testable**
- Prefer **explicitness over cleverness**
- Preserve **intentional design decisions** over perceived optimizations
- Assume this project will be resumed across sessions and by other agents
- Preserve continuity through **documentation, not inference**

---

## Coding Rules (Non‑Negotiable)

These rules apply globally and must be enforced before proposing changes.

- All code must be **Python 3.12+ compatible**
- Use **snake_case** for all variables, functions, and filenames
- Business logic MUST live in **pure functions** (no side effects)
- Dagster assets MUST be **thin wrappers** around pure logic
- Explicit dependencies only — no hidden imports or globals
- Do NOT introduce new frameworks, tools, or dependencies without instruction
- Do NOT rewrite working pipelines unless explicitly requested

---

## Testing & Quality Expectations (Enforced)

- Follow **pytest‑first Test‑Driven Development (TDD)**
- Write tests for:
  - transforms
  - validation rules
  - date / partition logic
  - env‑aware behavior
- Use `pytest.monkeypatch` to isolate:
  - time
  - environment variables
  - external services
- Tests MUST run in CI **without secrets, credentials, or external systems**

---

## MEMORY MANAGEMENT & CONTEXT CONTINUITY (CRITICAL)

This repository relies on **documents as long‑term memory**. You MUST treat documentation as an extension of your working context.

### Mental Model

- You do **not** retain persistent memory across sessions
- All continuity comes from **reading and updating project docs**
- If it is not written down, it will be lost

---

## Memory Sources (Priority Order)

When resuming work or extending functionality, you MUST consult the following files in order (if present):

1. **AGENTS.md** — behavior, constraints, and enforcement rules
2. **README.md** — project purpose and scope
3. **ARCHITECTURE.md** — system layout, boundaries, and data flow
4. **DECISIONS.md / ADRs** — frozen design decisions and rationale
5. **STATUS.md / TODO.md / Issues** — current state of work

You must NOT infer architecture, intent, or conventions without consulting these sources.

---

## Stable vs Flexible Design

Before making changes, classify the change explicitly.

### Stable (DO NOT change unless explicitly instructed)

- Data model grain
- Partitioning strategy
- Naming conventions
- Environment‑handling approach
- Separation of orchestration vs business logic

### Flexible (Safe to improve)

- Internal helper functions
- Refactors that preserve observable behavior
- Documentation clarity
- Test coverage improvements

If uncertain, ASK before proceeding.

---

## Decision Preservation Rules (Enforced)

- Agents MUST preserve prior decisions
- If a design choice appears suboptimal, consult `DECISIONS.md`
- Do NOT "fix" intentional tradeoffs
- To propose a change:
  - State the existing decision
  - Explain why it no longer holds
  - Propose an alternative
  - Wait for explicit approval

---

## Documentation as Memory (Required Behavior)

You MUST update documentation when:

- Introducing a new module, package, or folder
- Changing data flow, schema, or architecture
- Adding a new constraint, invariant, or convention
- Making a non‑obvious or irreversible tradeoff

Minimum expectations:

- Update `ARCHITECTURE.md` for structural changes
- Add an entry to `DECISIONS.md` for irreversible decisions
- Update `STATUS.md` when completing, blocking, or deferring work

Documentation updates MUST occur **in the same change set** as code.

---

## How to Write for Agent Memory

When editing or creating documentation:

- Prefer **bullet points over paragraphs**
- Use **imperative language** (MUST, DO NOT, ALWAYS)
- Put **constraints before explanations**
- Explicitly state **why** decisions exist
- Clearly distinguish **rules** from **guidelines**

Assume future agents will skim rather than read deeply.

---

## Safe Defaults When Context Is Missing

If documentation is incomplete or ambiguous:

1. Pause and ask for clarification
2. Propose options with explicit tradeoffs
3. Do NOT guess or silently re‑architect

---

## Output Expectations (Enforced)

- Prefer small, incremental, reversible changes
- Include tests with all new or modified logic
- Call out assumptions explicitly
- Ask before making breaking changes
- Keep solutions simple, readable, and well‑scoped

---

## Final Rule

> **Your primary job is to preserve intent over time.**
>
> Code solves today’s problem. Documentation prevents tomorrow’s agent from undoing it.

---

## Development Methodology

- Follow **Test‑Driven Development (TDD)**: Red → Green → Refactor
- Write the smallest failing test first
- Implement only what is required to pass the test
- Refactor ONLY when tests are green
- Apply **Tidy First** — never mix structural and behavioral changes
- Follow **SOLID principles**
- Prefer small, reversible commits

---

## Coding Standards

- Clear, descriptive naming
- Prefer pure functions
- Avoid duplication (DRY)
- Small, focused functions
- Explicit dependencies

### Python

- Type hints everywhere
- NumPy‑style docstrings for non‑trivial functions
- Import order: stdlib → third‑party → local
- No wildcard imports

---

## DataFrames

- Prefer **Polars**
- Use Pandas only when required
- Document joins and assumptions
- Keep method chaining readable

---

## Dagster (If Used)

- Prefer assets over ops
- One asset = one responsibility
- Keep logic in testable helpers
- Log row counts and key dimensions
- Attach meaningful metadata
- Always specify schedule timezones

---

## dbt (If Used)

- `stg_` → staging models
- `int_` → intermediate models
- `dim_` / `fct_` → final models
- Add tests and documentation
- Prefer readable SQL with CTEs

---

## dlt (If Used)

- Separate extraction, normalization, and loading
- Centralize environment‑aware configuration
- Document incremental keys and destinations

---

## Configuration & Secrets

- Never hard‑code secrets
- Use environment variables
- Provide `.env.example`
- Centralize LOCAL / QA / PROD behavior

---

## Commits

Only commit when:

- All tests pass
- The change represents a single logical unit

**Examples**

- `feat: add pricing normalization`
- `refactor: extract date parsing helper`

---

## Agent Interaction Rules

Agents MUST:

1. Propose changes using explicit diffs
2. Never apply changes without approval
3. Prefer minimal, safe changes
4. Respect repository conventions
5. Ask one focused clarification question when needed

---

## Non‑Goals

- No over‑engineering
- No silent behavior changes
- No mixed refactor + feature commits
- No failing tests
- No unnecessary dependencies

---

## Markdown Standards

- Follow markdownlint
- Keep content concise and scannable

---

## References

- Kent Beck — *Test‑Driven Development*
- Kent Beck — *Tidy First*
- Martin Fowler — *Refactoring*

