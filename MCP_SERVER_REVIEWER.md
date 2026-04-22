# Logiri Reviewer MCP Server

## Entry point

Run the MCP server over stdio with:

```powershell
node C:\Users\Owner\Documents\New project\bootstrap-v1\bin\reviewer-mcp.cjs
```

This Node wrapper uses the same MCP SDK transport style as the working `logiri` server and shells into Symfony/PHP for the actual reviewer payloads.

## Tools

### `morning_brief`

Inputs:

- `assignee` optional string
- `limit` optional integer

Returns an operator-ready board brief with:

- `do_now`
- `resolved_or_outdated_tasks`
- `board_actions`
- `wait_for_crawl`
- `likely_false_positives`
- `rules_to_revise`
- `suspected_duplicates`
- `infrastructure_blockers`
- `pipeline_status_guess`
- `operating_recommendation`

### `review_daily_summary`

Inputs:

- `assignee` optional string
- `limit` optional integer

Returns the same structured board-health summary used by:

- `GET /api/reviewer/daily-summary`
- `php bin/console app:review-board`

### `review_pending_tasks`

Inputs:

- `assignee` optional string
- `limit` optional integer
- `statuses` optional array of task statuses

Returns structured task reviews with:

- `verdict`
- `confidence`
- `reason_codes`
- `human_summary`
- `recommended_action`
- `rule_followup`
- `evidence`

### `review_task`

Inputs:

- `task_id` required integer

Returns the structured review for one task.

## Resources

### `logiri://reviewer/about`

Static metadata describing the reviewer server and its current toolset.

## Example MCP client config

Example JSON snippet for a client that launches MCP servers with a command plus args:

```json
{
  "mcpServers": {
    "logiri-reviewer": {
      "command": "node",
      "args": [
        "C:\\Users\\Owner\\Documents\\New project\\bootstrap-v1\\bin\\reviewer-mcp.cjs"
      ]
    }
  }
}
```

## Current scope

This server is intentionally read-only. It does not mutate tasks, rules, or learnings.

It is designed to help with:

- morning board review
- noisy-task triage
- rule QA
- task validity checks before execution
