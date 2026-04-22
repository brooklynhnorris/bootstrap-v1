#!/usr/bin/env node

const { spawnSync } = require('node:child_process');

const sdkRoot = 'C:/Users/Owner/Downloads/logiri-mvp/mcp-server/node_modules/@modelcontextprotocol/sdk/dist/cjs';
const { Server } = require(`${sdkRoot}/server/index.js`);
const { StdioServerTransport } = require(`${sdkRoot}/server/stdio.js`);
const { CallToolRequestSchema, ListToolsRequestSchema } = require(`${sdkRoot}/types.js`);

const SERVER_NAME = 'logiri-reviewer';
const SERVER_VERSION = '0.2.1';
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
