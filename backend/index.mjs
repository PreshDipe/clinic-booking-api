import crypto from 'node:crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, DeleteCommand,
  QueryCommand, ScanCommand, BatchWriteCommand, TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';

const API_KEY = process.env.API_KEY;
const TABLE = process.env.TABLE_NAME || 'clinic-booking';
const db = DynamoDBDocumentClient.from(new DynamoDBClient({})); // access via the Lambda's IAM role, no password

// Everything lives in one table. These are the "addresses" (keys) of each kind of record:
const SLOT    = (slotId) => ({ PK: 'SLOTS', SK: `SLOT#${slotId}` });          // all slots together
const BOOKING = (email, sk) => ({ PK: `EMAIL#${email}`, SK: sk });           // each person's bookings together, oldest first
const LOOKUP  = (bookingId) => ({ PK: `BOOKING#${bookingId}`, SK: 'INFO' });  // booking id -> where the booking is
const RETRY   = (key) => ({ PK: `RETRY#${key}`, SK: 'INFO' });                // saved answer for an Idempotency-Key
const SPAM    = (email) => ({ PK: `SPAM#${email}`, SK: 'INFO' });             // attempt counter per email


// ============================================================== 1. HANDLER

export const handler = async (event) => {
  try {
    const method = event.requestContext.http.method;
    const path = event.rawPath;
    const query = event.queryStringParameters || {};
    const headers = Object.fromEntries(Object.entries(event.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));

    if (method === 'OPTIONS') return { statusCode: 204, headers: CORS, body: '' };   // browser's CORS check
    if (path === '/health') return reply(200, { ok: true });

    // LEVEL 2: no valid API key, no entry.
    if (!isValidKey(headers['x-api-key'])) return error(401, 'missing or invalid API key');

    let body;
    if (event.body) {
      try { body = JSON.parse(event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString() : event.body); }
      catch { return error(400, 'request body is not valid JSON'); }
    }

    const id = path.startsWith('/bookings/') ? decodeURIComponent(path.slice('/bookings/'.length)) : null;

    if (method === 'POST'   && path === '/reset')    return await resetSlots(body);
    if (method === 'GET'    && path === '/slots')    return await listSlots(query);
    if (method === 'POST'   && path === '/bookings') return await createBooking(body, headers['idempotency-key']);
    if (method === 'GET'    && path === '/bookings') return await listBookings(query);
    if (method === 'GET'    && id)                   return await getBooking(id);
    if (method === 'DELETE' && id)                   return await cancelBooking(id);
    return error(404, 'not found');
  } catch (err) {
    console.error(err);                                         // details go to CloudWatch only
    return error(500, 'internal server error');
  }
};


// ============================================================== 2. ROUTES

// POST /reset (Level 1): delete everything, then save the new slots.
async function resetSlots(body) {
  const slots = body?.slots;
  if (!Array.isArray(slots)) return error(400, 'slots must be an array');
  for (const s of slots) {
    if (!s || !text(s.id) || typeof s.provider !== 'string' || !isDate(s.start) || !isDate(s.end)) {
      return error(400, 'each slot needs id, provider, start and end (date-time)');
    }
  }
  if (new Set(slots.map((s) => s.id)).size !== slots.length) return error(400, 'slot ids must be unique');

  const everything = await scanAll();
  await batchWrite(everything.map((Key) => ({ DeleteRequest: { Key } })));
  await batchWrite(slots.map((s) => ({ PutRequest: { Item: {
    ...SLOT(s.id), id: s.id, provider: s.provider, start: s.start, end: s.end, startMs: Date.parse(s.start),
  } } })));
  return reply(200, { ok: true, slots: slots.length });
}

// GET /slots (Level 1): every slot, earliest first. "available" = nobody has booked it.
async function listSlots(query) {
  const result = await db.send(new QueryCommand({
    TableName: TABLE, KeyConditionExpression: 'PK = :pk', ExpressionAttributeValues: { ':pk': 'SLOTS' }, ConsistentRead: true,
  }));
  let slots = result.Items;
  if (query.provider !== undefined) slots = slots.filter((s) => s.provider === query.provider);
  slots.sort((a, b) => a.startMs - b.startMs || a.id.localeCompare(b.id));
  return reply(200, { slots: slots.map((s) => ({ id: s.id, provider: s.provider, start: s.start, end: s.end, available: !s.bookingId })) });
}

// POST /bookings (Levels 1–5), in this order:
//   1. Level 4: a retry we've already answered? -> send the same answer again
//   2. Level 5: too many attempts from this email? -> 429
//   3. Levels 2–3: check the details, then book
async function createBooking(body, retryKey) {
  if (retryKey) {
    const retry = await startRetry(retryKey, body);
    if (retry.differentBody) return error(422, 'Idempotency-Key was already used with a different request body');
    if (retry.savedAnswer) return reply(retry.savedAnswer.status, retry.savedAnswer.body);
  }

  const email = cleanEmail(body?.email);
  if (email) {
    const waitSeconds = await countAttempt(email);
    if (waitSeconds) {
      if (retryKey) await forgetRetry(retryKey);
      return error(429, 'too many booking attempts for this email, try again later', { 'Retry-After': String(waitSeconds) });
    }
  }

  let answer;
  try {
    answer = await checkAndBook(body);
  } catch (err) {
    if (retryKey) await forgetRetry(retryKey);
    throw err;
  }
  if (retryKey) await saveRetryAnswer(retryKey, answer);
  return reply(answer.status, answer.body);
}

// LEVEL 2 checks (in the spec's order), then LEVEL 3 booking. Returns { status, body }.
async function checkAndBook(body) {
  const fail = (status, message) => ({ status, body: { error: message } });
  const slotId = body?.slot_id;
  const name = typeof body?.name === 'string' ? body.name.trim() : '';
  const email = cleanEmail(body?.email);

  if (!text(slotId))                              return fail(400, 'slot_id is required');
  if (!name)                                      return fail(400, 'name is required');
  if (!email)                                     return fail(400, 'email is required');
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))  return fail(400, 'email is not valid');

  const slot = await getItem(SLOT(slotId));
  if (!slot)                                      return fail(404, 'slot not found');
  if (slot.startMs <= Date.now())                 return fail(400, 'slot has already started');

  const booking = await bookSlot(slotId, name, email);
  return booking ? { status: 201, body: booking } : fail(409, 'slot is already booked');
}

// GET /bookings?email=&limit=&cursor= (Level 4): a person's bookings, oldest first, page by page.
async function listBookings(query) {
  const email = cleanEmail(query.email);
  if (!email) return error(400, 'email is required');

  const limit = query.limit === undefined ? 10 : Number(query.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 50 || !/^\d+$/.test(query.limit ?? '10')) {
    return error(400, 'limit must be an integer from 1 to 50');
  }

  // The cursor is the position (sort key) of the last booking on the previous page.
  const after = query.cursor ? Buffer.from(query.cursor, 'base64url').toString() : null;
  if (after !== null && !after.startsWith('BOOKING#')) return error(400, 'cursor is not valid');

  const result = await db.send(new QueryCommand({
    TableName: TABLE,
    KeyConditionExpression: 'PK = :pk',
    ExpressionAttributeValues: { ':pk': `EMAIL#${email}` },
    ExclusiveStartKey: after ? BOOKING(email, after) : undefined,
    Limit: limit + 1,                                 // one extra tells us if there is another page
    ConsistentRead: true,
  }));
  const page = result.Items.slice(0, limit);
  const more = result.Items.length > limit;
  return reply(200, {
    bookings: page.map(publicBooking),
    next_cursor: more ? Buffer.from(page[page.length - 1].SK).toString('base64url') : null,
  });
}

// GET /bookings/{id} (Level 1)
async function getBooking(id) {
  const found = await findBooking(id);
  return found ? reply(200, publicBooking(found)) : error(404, 'booking not found');
}

// DELETE /bookings/{id} (Level 3): cancel the booking and free its slot.
async function cancelBooking(id) {
  const found = await findBooking(id);
  if (!found) return error(404, 'booking not found');
  const cancelled = await freeSlot(found);
  return cancelled ? reply(200, { ...publicBooking(found), status: 'cancelled' }) : error(409, 'booking is already cancelled');
}


// ============================================================== 3. 
async function bookSlot(slotId, name, email) {
  const created_at = new Date().toISOString();
  const booking = { id: crypto.randomUUID(), slot_id: slotId, name, email, status: 'confirmed', created_at };
  const sk = `BOOKING#${created_at}#${booking.id}`;

  const ok = await transaction([
    { Update: { TableName: TABLE, Key: SLOT(slotId),
        UpdateExpression: 'SET bookingId = :id',
        ConditionExpression: 'attribute_not_exists(bookingId)',          // the "ONLY IF"
        ExpressionAttributeValues: { ':id': booking.id } } },
    { Put: { TableName: TABLE, Item: { ...BOOKING(email, sk), ...booking } } },
    { Put: { TableName: TABLE, Item: { ...LOOKUP(booking.id), email, sk } } },
  ]);
  return ok ? booking : null;
}

// Cancel = set status to "cancelled" ONLY IF it is still confirmed, and free the slot. Together.
async function freeSlot(booking) {
  return transaction([
    { Update: { TableName: TABLE, Key: BOOKING(booking.email, booking.SK),
        UpdateExpression: 'SET #s = :cancelled',
        ConditionExpression: '#s = :confirmed',                         // a second cancel fails here -> 409
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':cancelled': 'cancelled', ':confirmed': 'confirmed' } } },
    { Update: { TableName: TABLE, Key: SLOT(booking.slot_id), UpdateExpression: 'REMOVE bookingId' } },
  ]);
}

// LEVEL 4: a retried request never creates a duplicate booking.
async function startRetry(key, body) {
  const bodyHash = crypto.createHash('sha256').update(JSON.stringify(sortKeys(body ?? null))).digest('hex');
  for (let i = 0; i < 100; i++) {                                         // wait up to ~10 seconds
    const claimed = await putIfNew(RETRY(key), { bodyHash, ttl: inSeconds(86400) });
    if (claimed) return {};                                               // first time we see this key

    const saved = await getItem(RETRY(key));
    if (!saved) continue;                                                 // just released, try again
    if (saved.bodyHash !== bodyHash) return { differentBody: true };
    if (saved.answer) return { savedAnswer: saved.answer };
    await sleep(100);                                                     // first request still working
  }
  throw new Error('previous request with this Idempotency-Key is still running');
}
const saveRetryAnswer = (key, answer) => db.send(new UpdateCommand({
  TableName: TABLE, Key: RETRY(key), UpdateExpression: 'SET answer = :a', ExpressionAttributeValues: { ':a': answer },
}));
const forgetRetry = (key) => db.send(new DeleteCommand({ TableName: TABLE, Key: RETRY(key) }));

// LEVEL 5: at most 5 booking attempts per email per minute (failed ones count too).
async function countAttempt(email) {
  const now = Date.now();
  for (let i = 0; i < 5; i++) {
    try {
      // Add 1 to the current window, ONLY IF it is less than a minute old.
      const { Attributes: w } = await db.send(new UpdateCommand({
        TableName: TABLE, Key: SPAM(email),
        UpdateExpression: 'ADD attempts :one',
        ConditionExpression: 'windowStart > :minuteAgo',
        ExpressionAttributeValues: { ':one': 1, ':minuteAgo': now - 60000 },
        ReturnValues: 'ALL_NEW',
      }));
      return w.attempts <= 5 ? 0 : Math.max(1, Math.ceil((w.windowStart + 60000 - now) / 1000));
    } catch (err) {
      if (err.name !== 'ConditionalCheckFailedException') throw err;
    }
    // No window open: start one with this as attempt number 1 (ONLY IF nobody else just did).
    const started = await putIfNew(SPAM(email), { attempts: 1, windowStart: now, ttl: inSeconds(3600) }, now - 60000);
    if (started) return 0;
  }
  throw new Error('could not count booking attempt');
}


// ============================================================== 4. HELPERS

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Idempotency-Key',
  'Access-Control-Expose-Headers': 'Retry-After',
};
const reply = (statusCode, body, headers = {}) => ({
  statusCode, headers: { ...CORS, 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body),
});
const error = (statusCode, message, headers) => reply(statusCode, { error: message }, headers); // always { "error": ... }

// Compare in constant time, so the key can't be guessed from response times.
function isValidKey(given) {
  return Boolean(API_KEY) && typeof given === 'string' && given.length === API_KEY.length
    && crypto.timingSafeEqual(Buffer.from(given), Buffer.from(API_KEY));
}

const publicBooking = (b) => ({ id: b.id, slot_id: b.slot_id, name: b.name, email: b.email, status: b.status, created_at: b.created_at });

async function findBooking(id) {
  const lookup = await getItem(LOOKUP(id));
  return lookup ? getItem(BOOKING(lookup.email, lookup.sk)) : null;
}

async function getItem(Key) {
  return (await db.send(new GetCommand({ TableName: TABLE, Key, ConsistentRead: true }))).Item;
}

// Save ONLY IF the record doesn't exist (or, if `expiredBefore` is given, its window has expired).
async function putIfNew(key, data, expiredBefore) {
  try {
    await db.send(new PutCommand({
      TableName: TABLE, Item: { ...key, ...data },
      ConditionExpression: expiredBefore ? 'attribute_not_exists(PK) OR windowStart <= :t' : 'attribute_not_exists(PK)',
      ExpressionAttributeValues: expiredBefore ? { ':t': expiredBefore } : undefined,
    }));
    return true;
  } catch (err) {
    if (err.name === 'ConditionalCheckFailedException') return false;
    throw err;
  }
}

// Runs a transaction. Returns true if it went through, false if an "ONLY IF" check failed.
// If two transactions touch the same record at the same instant, DynamoDB asks us to retry.
async function transaction(steps) {
  for (let i = 0; i < 10; i++) {
    try {
      await db.send(new TransactWriteCommand({ TransactItems: steps }));
      return true;
    } catch (err) {
      const reasons = (err.CancellationReasons || []).map((r) => r?.Code);
      if (reasons.includes('ConditionalCheckFailed')) return false;
      if (!reasons.includes('TransactionConflict')) throw err;
      await sleep(10 + Math.random() * 40);
    }
  }
  return false;
}

async function scanAll() {
  const keys = [];
  let start;
  do {
    const page = await db.send(new ScanCommand({ TableName: TABLE, ProjectionExpression: 'PK, SK', ExclusiveStartKey: start }));
    keys.push(...page.Items);
    start = page.LastEvaluatedKey;
  } while (start);
  return keys;
}

// DynamoDB takes at most 25 writes per batch, and may hand some back to retry.
async function batchWrite(requests) {
  for (let i = 0; i < requests.length; i += 25) {
    let todo = { [TABLE]: requests.slice(i, i + 25) };
    while (todo) {
      const result = await db.send(new BatchWriteCommand({ RequestItems: todo }));
      todo = Object.keys(result.UnprocessedItems || {}).length ? result.UnprocessedItems : null;
      if (todo) await sleep(100);
    }
  }
}

const text = (v) => typeof v === 'string' && v.trim() !== '';
const isDate = (v) => typeof v === 'string' && !Number.isNaN(Date.parse(v));
const cleanEmail = (v) => (typeof v === 'string' ? v.trim().toLowerCase() : '');
const inSeconds = (s) => Math.floor(Date.now() / 1000) + s;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sortKeys = (v) => (Array.isArray(v) ? v.map(sortKeys)
  : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, sortKeys(v[k])])) : v);
