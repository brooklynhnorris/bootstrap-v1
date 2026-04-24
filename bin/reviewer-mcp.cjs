#!/usr/bin/env node

const { spawnSync } = require('node:child_process');

const sdkRoot = 'C:/Users/Owner/Downloads/logiri-mvp/mcp-server/node_modules/@modelcontextprotocol/sdk/dist/cjs';
const { Server } = require(`${sdkRoot}/server/index.js`);
const { StdioServerTransport } = require(`${sdkRoot}/server/stdio.js`);
const { CallToolRequestSchema, ListToolsRequestSchema } = require(`${sdkRoot}/types.js`);

const SERVER_NAME = 'logiri-reviewer';
const SERVER_VERSION = '0.2.2';
const PHP_PATH = 'C:/php84/php.exe';
const TOOL_SCRIPT = 'C:/Users/Owner/Documents/New project/bootstrap-v1/bin/reviewer-tool.php';

const tools = [
  {
    name: 'morning_brief',
    description: 'Return the operator-ready morning brief: do now, reject now, wait for crawl, likely false positives, resolved or outdated tasks, rules to revise, infrastructure blockers, suspected duplicates, and a one-paragraph recommendation.',
    inputSchema: {
      type: 'object',
      properties: {
        assignee: {
          type: 'string',
          description: 'Optional persona name such as Brook, Brad, Jeanne, or Kalib.',
        },
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 200,
          description: 'Maximum number of pending tasks to review while building the brief.',
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: 'review_daily_summary',
    description: 'Return a structured daily board-health summary with top noise signals, best work now, and cleanup queue.',
    inputSchema: {
      type: 'object',
      properties: {
        assignee: {
          type: 'string',
          description: 'Optional persona name such as Brook, Brad, Jeanne, or Kalib.',
        },
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 200,
          description: 'Maximum number of pending tasks to review while generating the summary.',
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: 'review_pending_tasks',
    description: 'Review pending or in-progress tasks against live rule, crawl, and rejection evidence and return structured verdicts.',
    inputSchema: {
      type: 'object',
      properties: {
        assignee: {
          type: 'string',
          description: 'Optional persona name used to filter the task list.',
        },
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 200,
          description: 'Maximum number of tasks to review.',
        },
        statuses: {
          type: 'array',
          items: {
            type: 'string',
            enum: ['pending', 'in_progress', 'done', 'closed'],
          },
          description: 'Optional list of task statuses to review.',
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: 'review_all_pending',
    description: 'Review the full pending board only and return structured per-task verdicts. Use this when you want every currently pending card reviewed without specifying statuses.',
    inputSchema: {
      type: 'object',
      properties: {
        assignee: {
          type: 'string',
          description: 'Optional persona name used to filter the pending board to one operator.',
        },
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 500,
          description: 'Maximum number of pending tasks to review.',
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: 'review_task',
    description: 'Review a single task by ID and return the structured reviewer verdict plus evidence.',
    inputSchema: {
      type: 'object',
      properties: {
        task_id: {
          type: 'integer',
          minimum: 1,
          description: 'Task ID to review.',
        },
      },
      required: ['task_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'propose_rule_gaps',
    description: 'Scan the active rule corpus plus current SEO/AI-readiness gaps and return net-new rule opportunities, strict proposal guidelines, success measures, and the exact prompt contract for future rule-gap reviews.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 20,
          description: 'Maximum number of candidate rule gaps to return.',
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: 'close_tasks',
    description: 'Close one or more tasks, persist the rejection memory, and optionally suppress regeneration scope when the rejection represents a real false positive.',
    inputSchema: {
      type: 'object',
      properties: {
        task_ids: {
          type: 'array',
          items: { type: 'integer', minimum: 1 },
          minItems: 1,
          description: 'Task IDs to close.',
        },
        reason_text: {
          type: 'string',
          description: 'Human-readable close reason to store on the task and in rejection memory.',
        },
        reason_code: {
          type: 'string',
          description: 'Structured reason code such as no_active_violation, malformed_task_url, or asset_url_false_positive.',
        },
        scope: {
          type: 'string',
          enum: ['task_only', 'url_only', 'rule_page_type', 'rule_global'],
          description: 'Suppression scope to apply when this represents a repeat false positive.',
        },
        actor: {
          type: 'string',
          description: 'Optional actor label to record in audit memory.',
        },
      },
      required: ['task_ids', 'reason_text'],
      additionalProperties: false,
    },
  },
  {
    name: 'submit_rule_feedback',
    description: 'Write structured reviewer feedback for a rule so future evaluation and proposal synthesis can learn from the current board state.',
    inputSchema: {
      type: 'object',
      properties: {
        rule_id: { type: 'string' },
        task_id: { type: 'integer', minimum: 1 },
        url: { type: 'string' },
        outcome_status: { type: 'string' },
        what_worked: { type: 'string' },
        what_didnt_work: { type: 'string' },
        proposed_change: { type: 'string' },
        change_type: { type: 'string' },
        actor: { type: 'string' },
      },
      required: ['rule_id'],
      additionalProperties: false,
    },
  },
  {
    name: 'revise_rule',
    description: 'Update an active rule directly in seo_rules and optionally attach a revision summary to rule_feedback.',
    inputSchema: {
      type: 'object',
      properties: {
        rule_id: { type: 'string' },
        changes: {
          type: 'object',
          description: 'Allowed keys: trigger_sql, trigger_condition, threshold, diagnosis, action_output, priority, assigned, is_active.',
        },
        summary: { type: 'string' },
        actor: { type: 'string' },
      },
      required: ['rule_id', 'changes'],
      additionalProperties: false,
    },
  },
  {
    name: 'trigger_crawl',
    description: 'Run a targeted crawl for specific URLs, a full HTML crawl, a WordPress refresh, or the full nightly refresh flow.',
    inputSchema: {
      type: 'object',
      properties: {
        mode: {
          type: 'string',
          enum: ['targeted', 'full', 'wordpress_refresh', 'nightly'],
        },
        urls: {
          type: 'array',
          items: { type: 'string' },
          description: 'Required for targeted mode.',
        },
        limit: {
          type: 'integer',
          minimum: 1,
          maximum: 1000,
          description: 'URL cap for full/nightly HTML crawl modes.',
        },
        sync_page_facts: {
          type: 'boolean',
          description: 'Whether to sync page_facts after the crawl step.',
        },
      },
      required: ['mode'],
      additionalProperties: false,
    },
  },
];

const server = new Server(
  {
    name: SERVER_NAME,
    version: SERVER_VERSION,
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  const safeArgs = args ?? {};

  try {
    const payload = callPhpTool(name, safeArgs);
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(payload, null, 2),
        },
      ],
      structuredContent: payload,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ error: message, tool: name }),
        },
      ],
      isError: true,
    };
  }
});

function callPhpTool(name, args) {
  const result = spawnSync(PHP_PATH, [TOOL_SCRIPT, name, JSON.stringify(args)], {
    encoding: 'utf8',
    windowsHide: true,
    env: {
      ...process.env,
      APP_ENV: process.env.APP_ENV || 'prod',
      APP_DEBUG: process.env.APP_DEBUG || '0',
    },
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || `PHP tool exited with code ${result.status}`).trim());
  }

  const output = (result.stdout || '').trim();
  if (!output) {
    throw new Error(`PHP tool returned no output for ${name}.`);
  }

  try {
    return JSON.parse(output);
  } catch (error) {
    const repaired = extractTrailingJsonObject(output);
    if (repaired !== null) {
      return repaired;
    }

    throw error;
  }
}

function extractTrailingJsonObject(output) {
  const candidates = [output.indexOf('{'), output.indexOf('[')].filter((index) => index >= 0);
  if (candidates.length === 0) {
    return null;
  }

  for (const start of candidates.sort((a, b) => a - b)) {
    const candidate = output.slice(start).trim();
    try {
      return JSON.parse(candidate);
    } catch (_) {
      // Keep trying later starts.
    }
  }

  return null;
}

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((error) => {
  console.error(`[${SERVER_NAME}] Fatal startup error:`, error);
  process.exit(1);
});
