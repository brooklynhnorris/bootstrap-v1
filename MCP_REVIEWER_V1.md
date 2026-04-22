# MCP Reviewer V1

## What landed

This repo now has an MCP-ready reviewer layer that can inspect pending tasks against:

- latest `page_facts` / `page_crawl_snapshots`
- active `rule_violations`
- structured `task_rejections`

The first slice is read-only. It does not auto-close, auto-reject, or auto-rewrite tasks.

## Core outputs

Each reviewed task returns:

- `verdict`: `do`, `reject`, `wait`, or `revise_rule`
- `confidence`
- `reason_codes`
- `human_summary`
- `recommended_action`
- `rule_followup`
- `evidence`

## API endpoints

### Review pending tasks

`GET /api/reviewer/tasks?assignee=Brook&limit=50&statuses=pending`

Optional `statuses` accepts a comma-separated list such as:

`pending,in_progress`

### Review a single task

`GET /api/reviewer/tasks/{id}`

### Daily summary

`GET /api/reviewer/daily-summary?assignee=Brook&limit=100`

Returns:

- board health counts
- top reason codes
- noisiest rules
- best work now
- cleanup queue

## Console usage

### Human-readable summary

```powershell
C:\php84\php.exe bin\console app:review-board --assignee=Brook --limit=100
```

### JSON summary

```powershell
C:\php84\php.exe bin\console app:review-board --assignee=Brook --limit=100 --json
```

## Why this matters

This is the server-side logic an MCP server can expose later as:

- tools for reviewing tasks
- resources for daily summaries
- structured evidence for rule QA

By keeping the logic in Symfony services first, the MCP layer can stay thin and deterministic.
