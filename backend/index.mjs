// Every rule that must hold under concurrency is a DynamoDB conditional write.
import crypto from 'node:crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand,
  QueryCommand, ScanCommand, BatchWriteCommand, TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';

const API_KEY = process.env.API_KEY;
const TABLE = process.env.TABLE_NAME || 'clinic-booking';
const RATE_LIMIT = 5;          // attempts per email ...
const RATE_WINDOW_MS = 60000;  // ... per minute
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const ddb = DynamoDBDocumentClient.from(
  new DynamoDBClient(process.env.DYNAMODB_ENDPOINT ? { endpoint: process.env.DYNAMODB_ENDPOINT } : {}),
  { marshallOptions: { removeUndefinedValues: true } },
);

// ---------------------------------------------------------------- HTTP helpers
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Idempotency-Key',
  'Access-Control-Expose-Headers': 'Retry-After',
};
const json = (statusCode, body, headers = {}) => ({
  statusCode, headers: { ...CORS, 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body),
});
const fail = (status, message, headers) => json(status, { error: message }, headers);

const isNonEmptyString = (v) => typeof v === 'string' && v.trim() !== '';
const isValidDate = (v) => typeof v === 'string' && !Number.isNaN(Date.parse(v));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = () => sleep(10 + Math.random() * 40);
const isCondFail = (e) => e.name === 'ConditionalCheckFailedException';
const reasons = (e) => (e.name === 'TransactionCanceledException' ? (e.CancellationReasons || []).map((r) => r && r.Code) : null);

// Same JSON regardless of key order / whitespace -> same hash (for Idempotency-Key).
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}
const hashBody = (body) => crypto.createHash('sha256').update(canonical(body ?? null)).digest('hex');

function keyIsValid(given) {
  if (!API_KEY || typeof given !== 'string' || given.length !== API_KEY.length) return false;
  return crypto.timingSafeEqual(Buffer.from(given), Buffer.from(API_KEY));
}

// ---------------------------------------------------------------- the handler
export const handler = async (event) => {
  try {
    // Works with API Gateway HTTP API (payload 2.0), REST API (1.0) and Function URLs.
    const method = (event.requestContext?.http?.method || event.httpMethod || 'GET').toUpperCase();
    let path = event.rawPath || event.path || '/';
    const stage = event.requestContext?.stage;
    if (stage && stage !== '$default' && path.startsWith(`/${stage}/`)) path = path.slice(stage.length + 1);
    if (path.length > 1) path = path.replace(/\/+$/, '');
    const headers = Object.fromEntries(Object.entries(event.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));
    const query = event.queryStringParameters || {};

    if (method === 'OPTIONS') return { statusCode: 204, headers: CORS, body: '' };
    if (method === 'GET' && path === '/health') return json(200, { ok: true });

    // Level 2: every request needs the API key.
    if (!keyIsValid(headers['x-api-key'])) return fail(401, 'missing or invalid API key');

    let body;
    if (event.body) {
      const text = event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body;
      try { body = JSON.parse(text); } catch { return fail(400, 'request body is not valid JSON'); }
    }

    if (method === 'POST' && path === '/reset') return await resetRoute(body);
    if (method === 'GET' && path === '/slots') return await slotsRoute(query);
    if (method === 'POST' && path === '/bookings') return await bookRoute(body, headers['idempotency-key']);
    if (method === 'GET' && path === '/bookings') return await listRoute(query);
    const m = path.match(/^\/bookings\/([^/]+)$/);
    if (m && method === 'GET') return await getRoute(decodeURIComponent(m[1]));
    if (m && method === 'DELETE') return await cancelRoute(decodeURIComponent(m[1]));
    return fail(404, 'not found');
  } catch (err) {
    console.error(err);
    return fail(500, 'internal server error');
  }
};

// ---------------------------------------------------------------- routes
// POST /reset (Level 1): delete every booking, replace all slots.
async function resetRoute(body) {
  const slots = body && body.slots;
  if (!Array.isArray(slots)) return fail(400, 'slots must be an array');
  for (const s of slots) {
    if (!s || !isNonEmptyString(s.id) || typeof s.provider !== 'string' || !isValidDate(s.start) || !isValidDate(s.end)) {
      return fail(400, 'each slot needs id, provider, start and end (date-time)');
    }
  }
  if (new Set(slots.map((s) => s.id)).size !== slots.length) return fail(400, 'slot ids must be unique');

  const deletes = [];
  let start;
  do {
    const page = await ddb.send(new ScanCommand({ TableName: TABLE, ProjectionExpression: 'PK, SK', ExclusiveStartKey: start }));
    for (const it of page.Items || []) deletes.push({ DeleteRequest: { Key: { PK: it.PK, SK: it.SK } } });
    start = page.LastEvaluatedKey;
  } while (start);
  await batchWrite(deletes);
  // start/end are stored exactly as sent, so they are echoed back unchanged.
  await batchWrite(slots.map((s) => ({ PutRequest: { Item: {
    PK: 'SLOTS', SK: `SLOT#${s.id}`, id: s.id, provider: s.provider, start: s.start, end: s.end, start_ts: Date.parse(s.start),
  } } })));
  return json(200, { ok: true, slots: slots.length });
}

// GET /slots?provider= (Level 1): every slot, earliest start first.
async function slotsRoute(query) {
  let items = [];
  let start;
  do {
    const page = await ddb.send(new QueryCommand({
      TableName: TABLE, KeyConditionExpression: 'PK = :p', ExpressionAttributeValues: { ':p': 'SLOTS' },
      ConsistentRead: true, ExclusiveStartKey: start,
    }));
    items.push(...(page.Items || []));
    start = page.LastEvaluatedKey;
  } while (start);
  if (query.provider !== undefined) items = items.filter((s) => s.provider === String(query.provider));
  items.sort((a, b) => a.start_ts - b.start_ts || (a.id < b.id ? -1 : 1));
  return json(200, { slots: items.map((s) => ({ id: s.id, provider: s.provider, start: s.start, end: s.end, available: !s.booking_id })) });
}

// POST /bookings (Levels 1–5)
async function bookRoute(body, idemKey) {
  const rawEmail = body && typeof body.email === 'string' ? body.email.trim().toLowerCase() : '';
  let claimed = false;
  try {
    // Level 4: same key + same body -> same response; same key + different body -> 422.
    if (idemKey) {
      const c = await claimIdempotency(idemKey, hashBody(body));
      if (c.mismatch) return fail(422, 'Idempotency-Key was already used with a different request body');
      if (c.replay) return json(c.replay.status, c.replay.body);
      claimed = true;
    }
    // Level 5: max 5 attempts per email per minute (failed attempts count).
    if (rawEmail) {
      const rl = await recordAttempt(rawEmail);
      if (!rl.allowed) {
        if (claimed) { await releaseIdempotency(idemKey); claimed = false; }
        return fail(429, 'too many booking attempts for this email, try again later', { 'Retry-After': String(rl.retryAfter) });
      }
    }
    const result = await createBooking(body);
    if (claimed) await completeIdempotency(idemKey, result.status, result.body);
    return json(result.status, result.body);
  } catch (err) {
    if (claimed) await releaseIdempotency(idemKey).catch(() => {});
    throw err;
  }
}

// Validation (in the spec's order) + the booking itself. Returns {status, body}.
async function createBooking(body) {
  const err = (status, message) => ({ status, body: { error: message } });
  if (!body || typeof body !== 'object' || Array.isArray(body)) return err(400, 'request body must be a JSON object');
  const { slot_id } = body;
  const name = typeof body.name === 'string' ? body.name.trim() : body.name;
  const email = typeof body.email === 'string' ? body.email.trim().toLowerCase() : body.email;

  if (!isNonEmptyString(slot_id)) return err(400, 'slot_id is required');
  if (!isNonEmptyString(name)) return err(400, 'name is required');
  if (!isNonEmptyString(email)) return err(400, 'email is required');
  if (!EMAIL_RE.test(email)) return err(400, 'email is not valid');

  const slot = (await ddb.send(new GetCommand({ TableName: TABLE, Key: { PK: 'SLOTS', SK: `SLOT#${slot_id}` }, ConsistentRead: true }))).Item;
  if (!slot) return err(404, 'slot not found');
  if (slot.start_ts <= Date.now()) return err(400, 'slot has already started');

  // Level 3: one transaction claims the slot ONLY IF it has no booking, and writes the booking.
  // However many requests arrive at the same instant, only one can satisfy the condition.
  const id = crypto.randomUUID();
  const created_at = new Date().toISOString();
  const sk = `BOOKING#${created_at}#${id}`;
  const booking = { id, slot_id, name, email, status: 'confirmed', created_at };
  for (let attempt = 0; attempt < 12; attempt++) {
    try {
      await ddb.send(new TransactWriteCommand({ TransactItems: [
        { Update: {
          TableName: TABLE, Key: { PK: 'SLOTS', SK: `SLOT#${slot_id}` }, UpdateExpression: 'SET booking_id = :b',
          ConditionExpression: 'attribute_exists(PK) AND attribute_not_exists(booking_id)', ExpressionAttributeValues: { ':b': id },
        } },
        { Put: { TableName: TABLE, Item: { PK: `EMAIL#${email}`, SK: sk, ...booking } } },
        { Put: { TableName: TABLE, Item: { PK: `BOOKING#${id}`, SK: 'BOOKING', email, sk } } },
      ] }));
      return { status: 201, body: booking };
    } catch (e) {
      const r = reasons(e);
      if (!r) throw e;
      if (r[0] === 'ConditionalCheckFailed') return err(409, 'slot is already booked');
      if (r.includes('TransactionConflict')) { await jitter(); continue; } // racing another booking: retry
      throw e;
    }
  }
  return err(409, 'slot is already booked');
}

// GET /bookings?email=&limit=&cursor= (Level 4): a person's bookings, oldest first.
async function listRoute(query) {
  const email = typeof query.email === 'string' ? query.email.trim().toLowerCase() : '';
  if (!email) return fail(400, 'email is required');
  let limit = 10;
  if (query.limit !== undefined) {
    const raw = String(query.limit);
    if (!/^\d+$/.test(raw) || Number(raw) < 1 || Number(raw) > 50) return fail(400, 'limit must be an integer from 1 to 50');
    limit = Number(raw);
  }
  let afterSk = null;
  if (query.cursor !== undefined && query.cursor !== '') {
    afterSk = Buffer.from(String(query.cursor), 'base64url').toString();
    if (!afterSk.startsWith('BOOKING#')) return fail(400, 'cursor is not valid');
  }
  const out = await ddb.send(new QueryCommand({
    TableName: TABLE, KeyConditionExpression: 'PK = :p', ExpressionAttributeValues: { ':p': `EMAIL#${email}` },
    ConsistentRead: true, ScanIndexForward: true, Limit: limit + 1, // one extra row tells us if there's another page
    ExclusiveStartKey: afterSk ? { PK: `EMAIL#${email}`, SK: afterSk } : undefined,
  }));
  const items = out.Items || [];
  const page = items.slice(0, limit);
  const next = items.length > limit ? Buffer.from(page[page.length - 1].SK).toString('base64url') : null;
  return json(200, { bookings: page.map(toBooking), next_cursor: next });
}

// GET /bookings/{id} (Level 1)
async function getRoute(id) {
  const found = await findBooking(id);
  return found ? json(200, toBooking(found.item)) : fail(404, 'booking not found');
}

// DELETE /bookings/{id} (Level 3): cancel and free the slot, atomically.
async function cancelRoute(id) {
  const found = await findBooking(id);
  if (!found) return fail(404, 'booking not found');
  const { item, key } = found;
  const markCancelled = {
    TableName: TABLE, Key: key, UpdateExpression: 'SET #s = :c', ConditionExpression: '#s = :conf',
    ExpressionAttributeNames: { '#s': 'status' }, ExpressionAttributeValues: { ':c': 'cancelled', ':conf': 'confirmed' },
  };
  for (let attempt = 0; attempt < 12; attempt++) {
    try {
      await ddb.send(new TransactWriteCommand({ TransactItems: [
        { Update: markCancelled },
        { Update: {
          TableName: TABLE, Key: { PK: 'SLOTS', SK: `SLOT#${item.slot_id}` }, UpdateExpression: 'REMOVE booking_id',
          ConditionExpression: 'booking_id = :b', ExpressionAttributeValues: { ':b': id },
        } },
      ] }));
      return json(200, { ...toBooking(item), status: 'cancelled' });
    } catch (e) {
      const r = reasons(e);
      if (!r) throw e;
      if (r[0] === 'ConditionalCheckFailed') return fail(409, 'booking is already cancelled');
      if (r.includes('TransactionConflict')) { await jitter(); continue; }
      if (r[1] === 'ConditionalCheckFailed') { // slot no longer held by this booking: just cancel the booking
        await ddb.send(new UpdateCommand(markCancelled)).catch((x) => { if (!isCondFail(x)) throw x; });
        return json(200, { ...toBooking(item), status: 'cancelled' });
      }
      throw e;
    }
  }
  throw new Error('cancel kept conflicting');
}

// ---------------------------------------------------------------- data helpers
const toBooking = (i) => ({ id: i.id, slot_id: i.slot_id, name: i.name, email: i.email, status: i.status, created_at: i.created_at });

async function findBooking(id) {
  const ptr = (await ddb.send(new GetCommand({ TableName: TABLE, Key: { PK: `BOOKING#${id}`, SK: 'BOOKING' }, ConsistentRead: true }))).Item;
  if (!ptr) return null;
  const key = { PK: `EMAIL#${ptr.email}`, SK: ptr.sk };
  const item = (await ddb.send(new GetCommand({ TableName: TABLE, Key: key, ConsistentRead: true }))).Item;
  return item ? { item, key } : null;
}

async function batchWrite(requests) {
  for (let i = 0; i < requests.length; i += 25) {
    let pending = { [TABLE]: requests.slice(i, i + 25) };
    for (let attempt = 0; pending; attempt++) {
      if (attempt > 8) throw new Error('DynamoDB batch write kept failing');
      if (attempt) await sleep(50 * 2 ** attempt);
      const out = await ddb.send(new BatchWriteCommand({ RequestItems: pending }));
      pending = out.UnprocessedItems && Object.keys(out.UnprocessedItems).length ? out.UnprocessedItems : null;
    }
  }
}

// Level 4: claim the key with a conditional put. A concurrent retry with the same key
// waits for the first request to finish, then gets exactly the same stored response.
async function claimIdempotency(idemKey, hash) {
  const Key = { PK: `IDEM#${idemKey}`, SK: 'IDEM' };
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    try {
      await ddb.send(new PutCommand({
        TableName: TABLE, ConditionExpression: 'attribute_not_exists(PK)',
        Item: { ...Key, hash, state: 'pending', ttl: Math.floor(Date.now() / 1000) + 86400 },
      }));
      return { claimed: true };
    } catch (e) { if (!isCondFail(e)) throw e; }
    const it = (await ddb.send(new GetCommand({ TableName: TABLE, Key, ConsistentRead: true }))).Item;
    if (!it) continue;                               // released meanwhile: try to claim again
    if (it.hash !== hash) return { mismatch: true };
    if (it.state === 'done') return { replay: { status: it.status_code, body: it.response } };
    await sleep(100);                                // first request still running
  }
  throw new Error('idempotent request is still in progress');
}
const completeIdempotency = (idemKey, status, body) => ddb.send(new UpdateCommand({
  TableName: TABLE, Key: { PK: `IDEM#${idemKey}`, SK: 'IDEM' },
  UpdateExpression: 'SET #st = :d, status_code = :s, #r = :r',
  ExpressionAttributeNames: { '#st': 'state', '#r': 'response' }, ExpressionAttributeValues: { ':d': 'done', ':s': status, ':r': body },
}));
const releaseIdempotency = (idemKey) => ddb.send(new DeleteCommand({ TableName: TABLE, Key: { PK: `IDEM#${idemKey}`, SK: 'IDEM' } }));

// Level 5: at most RATE_LIMIT attempts per email per RATE_WINDOW_MS. The version check
// (optimistic locking) means parallel requests can't both slip past the count.
async function recordAttempt(email) {
  const Key = { PK: `RATE#${email}`, SK: 'RATE' };
  for (let attempt = 0; attempt < 40; attempt++) {
    const now = Date.now();
    const it = (await ddb.send(new GetCommand({ TableName: TABLE, Key, ConsistentRead: true }))).Item;
    const recent = ((it && it.attempts) || []).filter((t) => t > now - RATE_WINDOW_MS).sort((a, b) => a - b);
    if (recent.length >= RATE_LIMIT) {
      const freesAt = recent[recent.length - RATE_LIMIT] + RATE_WINDOW_MS;
      return { allowed: false, retryAfter: Math.max(1, Math.ceil((freesAt - now) / 1000)) };
    }
    const version = (it && it.version) || 0;
    try {
      await ddb.send(new PutCommand({
        TableName: TABLE,
        Item: { ...Key, attempts: [...recent, now], version: version + 1, ttl: Math.floor(now / 1000) + 3600 },
        ConditionExpression: it ? 'version = :v' : 'attribute_not_exists(PK)',
        ExpressionAttributeValues: it ? { ':v': version } : undefined,
      }));
      return { allowed: true };
    } catch (e) {
      if (!isCondFail(e)) throw e;
      await jitter(); // someone else updated the counter first: re-read and retry
    }
  }
  throw new Error('rate limiter is busy');
}
