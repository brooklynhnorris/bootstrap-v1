# Logiri V2 Rollout

## Purpose

This document captures the first production cut of the Logiri v2 backend shape:

- normalized signals
- deterministic rule evaluation
- safer play and action handling
- reduced controller sprawl

It is written for this repo as it exists today, not as a greenfield redesign.

## Goals

The original system mixed too many concerns inside `HomeController`:

- schema creation
- analytics reads
- crawl reads
- prompt generation
- Claude API calls
- task creation
- destructive action handling

The v2 direction is:

1. ingest source data into normalized facts
2. evaluate deterministic rules against those facts
3. let the LLM explain, prioritize, and propose
4. validate writes server-side before they become tasks or actions

## What Landed

### New tables

Added through Doctrine migrations:

- `data_sources`
- `page_facts`
- `rule_violations`
- `action_requests`

Additional runtime-schema pressure was also moved toward migrations for:

- `activity_log`
- `custom_rules`
- `suppressed_tasks`
- `chat_learnings`

### New commands

- `php bin/console app:sync-page-facts`
- `php bin/console app:evaluate-foundational-rules`

### New services

- `PageFactsSyncService`
- `RuleEvaluationService`
- `ActionRequestService`
- `ConversationService`
- `RuleContextService`
- `ClaudeChatService`
- `PromptBuilderService`
- `TaskSuggestionService`
- `LearningExtractionService`
- `CrawlContextService`
- `ViolationSnapshotService`

### Controller refactor

`HomeController` now acts more like an orchestrator than a monolith.

The live chat/play path now delegates:

- prompt building
- Claude requests
- conversation persistence
- crawl context loading
- task creation
- learning extraction
- action queueing

Dead or dangerous legacy behaviors were removed or neutralized, including:

- direct LLM action execution
- controller-owned prompt builder path
- controller-owned learning extraction path
- most runtime schema setup behavior

## First End-to-End Slice

The first complete v2 slice is:

1. source data arrives in `page_crawl_snapshots` and `ga4_snapshots`
2. `app:sync-page-facts` normalizes into `page_facts`
3. `app:evaluate-foundational-rules` writes deterministic FC violations into `rule_violations`
4. chat/play generation reads those violations for prompt context
5. task creation validates rule-targeted plays against the active violation snapshot

This slice is intentionally narrow. It is focused on foundational content rules and safer execution behavior.

## Current Rule Coverage

The deterministic evaluator currently covers:

- `FC-R1`
- `FC-R3`
- `FC-R5`
- `FC-R7`
- `FC-R8`
- `FC-R9`
- `FC-R10`

These run against `page_facts`, not directly against raw crawl rows.

## Current Safety Improvements

### Model and auth safety

- `CLAUDE_MODEL` fallback supported alongside `ANTHROPIC_MODEL`
- API auth expiry responses are more reliably JSON for frontend callers

### Action safety

- LLM-proposed actions are queued into `action_requests`
- actions require approval/rejection flow
- supported approved actions execute server-side, not from arbitrary model output

### Task safety

- task creation still accepts `TASKS_JSON`, but now validates more aggressively
- duplicate and suppression checks remain
- rule-targeted plays must align with the active `rule_violations` snapshot
- impression lookup prefers normalized facts before raw crawl fallback

### Schema safety

- runtime DDL inside controller hot paths has been disabled or moved toward migrations

## Render / Environment Notes

This repo is deployed on Render, but local verification used:

- local code checkout
- Render Postgres credentials via `.env.local`
- `C:\php84\php.exe`

Important distinction:

- code changes do not affect the live Render UI until deployed
- database-connected console commands can be verified locally against the shared Render DB

## Commands

### Baseline refresh

```powershell
php bin/console doctrine:migrations:migrate
php bin/console app:sync-page-facts
php bin/console app:evaluate-foundational-rules
```

### Recommended sequence after a crawl

```powershell
php bin/console app:crawl-pages
php bin/console app:sync-page-facts
php bin/console app:evaluate-foundational-rules
```

If GA4 has refreshed separately, keep `ga4_snapshots` current before syncing page facts.

## Verified State

At the time of this rollout doc:

- migrations succeeded
- `app:sync-page-facts` succeeded
- `app:evaluate-foundational-rules` succeeded
- Symfony container lint passed

Observed successful snapshot:

- `845` page facts synced
- `528` rule violations stored in snapshot `1`

Those counts will change as new crawl/analytics data arrives.

## What Improved

### Before

- controller mixed facts, rules, recommendations, and writes
- model output could drive destructive behavior too directly
- rules were often inferred ad hoc from raw rows
- play/task generation could drift from actual current failures

### After first cut

- facts are normalized
- rules are explicitly stored
- prompt generation can reference deterministic violations
- task creation can reject stale/non-matching rule plays
- actions are proposed, queued, and reviewable

This is not the final architecture, but it is a meaningful move from heuristic orchestration to deterministic evaluation plus guarded generation.

## Remaining Risks

### 1. Prompt layer still influences too much

The deterministic layer now exists, but some play quality problems still live in prompt behavior:

- over-prescribed plays on stale snapshots
- occasional geo-targeting assumptions
- triage not always expressed as strongly as intended

### 2. Legacy fallback paths still exist

Some read paths still fall back to raw crawl or legacy tables when deterministic data is absent.

This is useful for resilience, but it means behavior can still vary depending on data freshness.

### 3. Limited deterministic coverage

Only the first foundational rules are deterministic today.

Large parts of recommendations, prioritization, and strategy are still prompt-led.

### 4. Limited test coverage

The refactor has been linted and exercised via console runs, but it still needs focused automated tests.

## Minimum Viable Next Slices

### Slice 2: Current violation read layer

Goal:

- expose current `rule_violations` more directly to backend/UI consumers

Status:

- partially done via `ViolationSnapshotService`

Next:

- add a small read/admin API for current snapshot inspection

### Slice 3: Triage hardening

Goal:

- ensure low-impression or zero-impression pages do not generate oversized execution plays

Next:

- harden strategic-review vs optimize behavior after fresh crawl data

### Slice 4: UI/admin visibility

Goal:

- inspect deterministic state in-app

Next:

- current violations view
- pending action requests view

### Slice 5: Tests

Add focused tests for:

- `PageFactsSyncService`
- `RuleEvaluationService`
- `TaskSuggestionService`
- `ViolationSnapshotService`

### Slice 6: Deeper legacy cleanup

Goal:

- reduce remaining raw crawl fallback and heuristic duplication

## Practical Rollout Guidance

### Safe rollout pattern

1. deploy code
2. run migrations
3. run page-fact sync
4. run rule evaluation
5. check live Render behavior
6. compare generated plays against actual current snapshot data

### What to watch in live behavior

- are generated plays tied to real URLs?
- do rule-based plays correspond to current deterministic failures?
- do low-value pages get strategic review instead of oversized execution work?
- are actions being queued instead of directly executed?

## Summary

The first v2 cut has landed.

The repo now has:

- normalized page facts
- deterministic foundational rule evaluation
- safer action handling
- safer task validation
- a materially smaller and less dangerous `HomeController`

The system is not fully finished, but it has crossed the line from “controller-driven heuristic app” to “deterministic facts and rules with LLM orchestration on top.”
