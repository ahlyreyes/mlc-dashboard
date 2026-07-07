require('dotenv').config();
const express = require('express');
const path = require('path');
const https = require('https');
const session = require('express-session');
const passport = require('passport');
const { Strategy: GoogleStrategy } = require('passport-google-oauth20');

const app = express();
const PORT = process.env.PORT || 8080;

const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || '';
const META_TOKEN_1     = process.env.META_TOKEN_1     || META_ACCESS_TOKEN;
const META_TOKEN_2     = process.env.META_TOKEN_2     || META_ACCESS_TOKEN;
const META_TOKEN_HUSSE = process.env.META_TOKEN_HUSSE || META_ACCESS_TOKEN;
// Current portfolio token — covers all active ad accounts (Clear Sight, CanPro, Fixora, HearWell)
const META_TOKEN_MAIN  = process.env.META_TOKEN_MAIN  || 'EAAamYRVUt6ABR9WKMojnjQfZCP2v3JDZBNFzdIitsBbTDquf6AZB4ZANZAaQlnBRTWdUwUfNEWmdrrdiptgm3BUnbFdEjU4w1NkTlYDRqurE4BZAXLVeRVRltRQR1jsr5yjSams2gZChVh9cnZAqdR6Oq4Cl7cLdB7nDlCLVwxLIVaLIj9a8smoU0RMMsAfYZCLmdagZDZD';

// Pancake POS API — live order / delivery status (key + shop from Railway env vars)
const PANCAKE_POS_KEY  = process.env.PANCAKE_POS_KEY  || '';
const PANCAKE_POS_SHOP = process.env.PANCAKE_POS_SHOP || '';

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || '';
const BASE_URL = process.env.BASE_URL || 'https://sellershub-fsd.com';

// Authorized users
const AUTHORIZED_USERS = [
  { email: 'ronatocharlonejrs@gmail.com',          name: 'Charlone',         role: 'admin' },
  { email: 'ahlyssar.work@gmail.com',              name: 'Aly',              role: 'admin' },
  { email: 'advertisingspecialist5ejay@gmail.com', name: 'Advertiser Ejay',  role: 'advertiser' },
  { email: 'advertisingspecialist6husse@gmail.com',name: 'Advertiser Husse', role: 'advertiser' },
  { email: 'adstafflaila@gmail.com',               name: 'Advertiser Angelika', role: 'advertiser' },
  { email: 'jackcruz1117@gmail.com',               name: 'Advertiser Jack',  role: 'advertiser' },
  { email: 'sczezsa@gmail.com',                    name: 'FSA Che',          role: 'advertiser' },
  { email: 'sfahovey03@gmail.com',                 name: 'FSA Hovey',        role: 'advertiser' },
  { email: 'johnericc1234@gmail.com',              name: 'Ericson',          role: 'admin' },
  { email: 'reyes.ahlyssa04@gmail.com',            name: 'Aly',              role: 'admin' },
  { email: 'crismarkreyes49@gmail.com',            name: 'Cris Mark',        role: 'admin' },
  { email: 'anglnslls1234@gmail.com',              name: 'FSA Angeline',     role: 'advertiser' },
];

// Users added at runtime via Admin Settings (persisted in the app_users DB table)
let dbUsers = [];
const normEmail = e => (e || '').trim().toLowerCase();
const SEED_EMAILS = new Set(AUTHORIZED_USERS.map(u => normEmail(u.email)));
function findUser(email) {
  const e = normEmail(email);
  return AUTHORIZED_USERS.find(u => normEmail(u.email) === e) ||
         dbUsers.find(u => normEmail(u.email) === e) || null;
}
function isAdmin(user) { return !!user && String(user.role || '').toLowerCase() === 'admin'; }

// PostgreSQL setup
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Initialize tables
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS action_logs (
        id SERIAL PRIMARY KEY,
        campaign_key TEXT NOT NULL,
        campaign_name TEXT,
        ad_account TEXT,
        date TEXT,
        recommendation TEXT,
        action_taken TEXT NOT NULL,
        new_budget NUMERIC,
        notes TEXT,
        done_by_email TEXT NOT NULL,
        done_by_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        locked BOOLEAN DEFAULT TRUE
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS page_budgets (
        id SERIAL PRIMARY KEY,
        page_key TEXT NOT NULL,
        date TEXT NOT NULL,
        budget NUMERIC,
        locked BOOLEAN DEFAULT FALSE,
        set_by_email TEXT,
        set_by_name TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(page_key, date)
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS creatives (
        id SERIAL PRIMARY KEY,
        campaign_key TEXT NOT NULL,
        link TEXT NOT NULL,
        title TEXT,
        submitted_by_email TEXT NOT NULL,
        submitted_by_name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        created_by TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    // Add locked column if it doesn't exist (migration)
    await pool.query(`ALTER TABLE page_budgets ADD COLUMN IF NOT EXISTS locked BOOLEAN DEFAULT FALSE`);
    await loadDbUsers();
    console.log('✅ DB tables ready');
  } catch(e) {
    console.error('DB init error:', e.message);
  }
}
initDB();

// Session + Passport
app.use(session({
  secret: process.env.SESSION_SECRET || 'snowball-ndap-secret-2026',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 1 * 60 * 60 * 1000 } // 1 hour
}));
app.use(passport.initialize());
app.use(passport.session());
app.use(express.json());

passport.use(new GoogleStrategy({
  clientID: GOOGLE_CLIENT_ID,
  clientSecret: GOOGLE_CLIENT_SECRET,
  callbackURL: BASE_URL + '/auth/google/callback'
}, (accessToken, refreshToken, profile, done) => {
  const email = profile.emails?.[0]?.value;
  const user = findUser(email);
  if (!user) return done(null, false, { message: 'unauthorized' });
  return done(null, { ...user, googleId: profile.id, photo: profile.photos?.[0]?.value });
}));

passport.serializeUser((user, done) => done(null, user.email));
passport.deserializeUser((email, done) => {
  done(null, findUser(email) || false);
});

const requireAuth = (req, res, next) => {
  if (req.isAuthenticated()) return next();
  res.redirect('/login');
};
const requireAdmin = (req, res, next) => {
  if (req.isAuthenticated() && isAdmin(req.user)) return next();
  if (req.accepts('html') && !req.xhr) return res.redirect('/home');
  res.status(403).json({ error: 'Admin only' });
};

async function loadDbUsers() {
  try {
    const r = await pool.query('SELECT email, name, role FROM app_users ORDER BY created_at');
    dbUsers = r.rows;
    console.log(`✅ Loaded ${dbUsers.length} DB user(s)`);
  } catch(e) { console.warn('loadDbUsers error:', e.message); }
}
const PANCAKE_CSV_URLS = [
  process.env.PANCAKE_CSV_URL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRRmBJlKTC1iFdU5mcZ8sQlWkHuxAtYxezNnAO1ggj1wKh1_ki045CTbDw6aV2FvVL5tBV42gMHilio/pub?gid=0&single=true&output=csv',
  process.env.PANCAKE_CSV_URL_APRIL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSWfevqFhSyLoIFwvwFFdgFY3NzyhTOu6nbW3_2CfhI460Etz60TPWH2yA1TkVfG2y439O43BOvXHb4/pub?gid=0&single=true&output=csv',
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vQKR8ZYu_ov1xrnk99ronJjmnnMMJqJ9orMR5LJDLUT35K4CzUYKW84ryywFg-K9rTQayZbEIY5PrBr/pub?output=csv', // May 2026
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vS80NfIGrjxEXGi-KN4hxYh5GlMxlWPmxco7OchDT29n9nm_fCuyJyuL9auyXa2iAx7yBUv75aPDgs3/pub?output=csv', // June 2026
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vR6FNkNs9U2PaZ6w_J68GAhwDsP2K3AQGJ9OaVWFczNLS-4WqRRZ6XS7UqIn0wId30jFn97Hq5N4Mdh/pub?output=csv', // July 2026
];

// MLC mainfile — source for AOV & CVR FSA report (John Hovey Cabatic, Lex Dela Cruz)
// Monthly MLC mainfile CSVs — add a new URL each month
const MLC_MAINFILE_CSV_URLS = [
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vRTziENAURT5p8ix0v0FOizV9a_i-p4Igeovw21jv09aqbJbqvsjKMEftGVfG8Dm0rmVvcKUv0MQkul/pub?output=csv', // April 2026
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vQKR8ZYu_ov1xrnk99ronJjmnnMMJqJ9orMR5LJDLUT35K4CzUYKW84ryywFg-K9rTQayZbEIY5PrBr/pub?output=csv',   // May 2026
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vS80NfIGrjxEXGi-KN4hxYh5GlMxlWPmxco7OchDT29n9nm_fCuyJyuL9auyXa2iAx7yBUv75aPDgs3/pub?output=csv',   // June 2026
  'https://docs.google.com/spreadsheets/d/e/2PACX-1vR6FNkNs9U2PaZ6w_J68GAhwDsP2K3AQGJ9OaVWFczNLS-4WqRRZ6XS7UqIn0wId30jFn97Hq5N4Mdh/pub?output=csv',   // July 2026
];

// Per-page Pancake access tokens (page-scoped, longer-lived)
const PANCAKE_PAGE_TOKENS = {
  '183224001550935': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjE4MzIyNDAwMTU1MDkzNSIsInRpbWVzdGFtcCI6MTc3ODA4ODAwMH0.KT2svFupRuq_bJiHKrysIR0pLyGIKBKQ1CPLyLeiLUI', // CS OPTICAL CARE
  '562290783624265': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjU2MjI5MDc4MzYyNDI2NSIsInRpbWVzdGFtcCI6MTc3NzIxNDY2NH0.xRs7f8JyPd_8DApwbTaV78MfYmzfskHKrcg6PkMPjJs', // CS EYE DROPS
};
// Fallback user-level token
const PANCAKE_TOKEN = process.env.PANCAKE_TOKEN || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbmZvIjp7Im9zIjoxLCJjbGllbnRfaXAiOiI2NC4yMjQuOTcuMTU0IiwiYnJvd3NlciI6MSwiZGV2aWNlX3R5cGUiOjN9LCJuYW1lIjoiQ2xhcmljZSBEYWxvbmRvbmFuIiwiZXhwIjoxNzgyMzA1MTQzLCJhcHBsaWNhdGlvbiI6MSwidWlkIjoiMWNjYjM3YTgtZjQ4NS00ZjdiLWJiMWQtZjhjNTQ0Nzg4NWM2Iiwic2Vzc2lvbl9pZCI6ImQ3MDM5OWFhLTIxYTMtNDRmZi05ZmI5LWVmMDBjNzI3YmE2YSIsImlhdCI6MTc3NDUyOTE0MywiZmJfaWQiOiIxMjIxMTI2MzIwNDg5OTY4NTUiLCJsb2dpbl9zZXNzaW9uIjpudWxsLCJmYl9uYW1lIjoiQ2xhcmljZSBEYWxvbmRvbmFuIn0.FUzLeVPKVMDqbruljozSc93SBsX76gj0HMfeiv4kpAA';
function pageToken(pageId) { return PANCAKE_PAGE_TOKENS[pageId] || PANCAKE_TOKEN; }

// Pages used for per-FSA inquiry count (v2 API, both combined)
const CS_INQUIRY_PAGES = [
  { pageId: '183224001550935', token: PANCAKE_PAGE_TOKENS['183224001550935'] }, // CS OPTICAL CARE
  { pageId: '562290783624265', token: PANCAKE_PAGE_TOKENS['562290783624265'] }, // CS EYE DROPS
];

// Fetch conversations via pages.fm v2 API — returns current_assign_users per conv
async function fetchPancakeConvsV2(pageId, token, sinceTs, untilTs) {
  const cacheKey = `conv_v2_${pageId}_${sinceTs}_${untilTs}`;
  const cached = cacheGet(pancakeConvCache, cacheKey, TTL_PANCAKE_CONV);
  if (cached) return cached;

  const allConvs = [];
  let lastId = null;
  let loops = 0;
  while (true) {
    let url = `https://pages.fm/api/public_api/v2/pages/${pageId}/conversations` +
      `?page_access_token=${token}&type=INBOX&since=${sinceTs}&until=${untilTs}&limit=60`;
    if (lastId) url += `&last_conversation_id=${lastId}`;
    try {
      const res = await fetchJson(url);
      const convs = res.conversations || res.data || [];
      allConvs.push(...convs);
      if (convs.length < 60) break; // partial page = last page (API has no has_more field)
      lastId = convs[convs.length - 1].id;
    } catch(e) {
      console.warn(`Pancake v2 conv fetch error ${pageId}: ${e.message}`);
      break;
    }
    if (++loops >= 500) break;
  }

  cacheSet(pancakeConvCache, cacheKey, allConvs);
  return allConvs;
}

// Fuzzy name match: true if either name is a substring of the other (lowercase, no spaces)
function fsaNameMatch(a, b) {
  const na = (a || '').toLowerCase().replace(/\s+/g, '');
  const nb = (b || '').toLowerCase().replace(/\s+/g, '');
  return na.length > 0 && nb.length > 0 && (na.includes(nb) || nb.includes(na));
}

// Clear Sight main FSAs — shown first in the report; other sellers appear after
const CS_FSA_PRIORITY = ['John Hovey Cabatic', 'Lex Dela Cruz'];

// All active Clear Sight Pancake page IDs — always fetch conversations from these for SDI count
const CS_ALL_PAGE_IDS = [
  '183224001550935', // CS OPTICAL CARE
  '105504325986879', // CS ESSENTIALS
  '343948545460634', // CS HUB
  '803157239549242', // CLEAR VISION
  '562290783624265', // CS EYE DROPS
  '937162626137875', // CS EYE RELIEF
  '731295746741259', // EYECARE HUB
  '778772685314844', // CS CATARACT
  '309583448901656', // CS CATARACT CARE
  '541578155698224', // CS EYE CARE
  '132190129971044', // CLEARSIGHT MNL
  '298354383369379', // CLEAR EYE SIGHT
];

// Map from Facebook Page name (lowercase) in POS CSV → Pancake page ID
const POS_PAGE_ID_MAP = {
  'clear sight optical care':   '183224001550935',
  'clear sight essentials':     '105504325986879',
  'clear sight hub':            '343948545460634',
  'clear vision solution':      '803157239549242',
  'clear sight eye drops':      '562290783624265',
  'clear sight eye drops ph':   '562290783624265',
  'clear sight eye relief':     '937162626137875',
  'clear sight eye relief ph':  '937162626137875',
  'eyecare hub':                '731295746741259',
  'clear sight cataract relief':'778772685314844',
  'clear sight cataract care':  '309583448901656',
  'clear sight - eye care':     '541578155698224',
  'clearsight mnl':             '132190129971044',
  'clear eye sight ph':         '298354383369379',
};

const PANCAKE_PAGE_META = {
  '183224001550935': { short: 'CS OPTICAL CARE' },
  '105504325986879': { short: 'CS ESSENTIALS' },
  '343948545460634': { short: 'CS HUB' },
  '803157239549242': { short: 'CLEAR VISION' },
  '562290783624265': { short: 'CS EYE DROPS' },
  '937162626137875': { short: 'CS EYE RELIEF' },
  '731295746741259': { short: 'EYECARE HUB' },
  '778772685314844': { short: 'CS CATARACT' },
  '309583448901656': { short: 'CS CATARACT CARE' },
  '541578155698224': { short: 'CS EYE CARE' },
  '132190129971044': { short: 'CLEARSIGHT MNL' },
  '298354383369379': { short: 'CLEAR EYE SIGHT' },
};

const pancakeConvCache = new Map();
const TTL_PANCAKE_CONV = 5 * 60 * 1000;

function normCxName(n) {
  return (n || '').toLowerCase().trim().replace(/\s+/g, ' ');
}

// Normalize phone to last 10 digits for comparison
function normPhone(p) {
  if (!p) return '';
  return (p + '').replace(/\D/g, '').slice(-10);
}

// Convert a UTC ISO timestamp to Philippine Time (UTC+8) date string YYYY-MM-DD
function toPHTDateStr(isoStr) {
  if (!isoStr) return '';
  const pht = new Date(new Date(isoStr).getTime() + 8 * 60 * 60 * 1000);
  return pht.toISOString().split('T')[0];
}

// Extract phone from a Pancake conversation object
function getConvPhone(conv) {
  const customers = conv.customers || [];
  for (const cu of customers) {
    if (cu && cu.phone) return cu.phone;
  }
  if (conv.from && conv.from.phone) return conv.from.phone;
  return null;
}

// Extract display name from a Pancake conversation object
function getConvName(conv) {
  return (conv.from && conv.from.name) ||
         (conv.customers && conv.customers[0] && conv.customers[0].name) || '';
}

async function fetchPancakeConvs(pageId, fromDate, toDate) {
  const cacheKey = `conv_${pageId}_${fromDate}_${toDate}`;
  const cached = cacheGet(pancakeConvCache, cacheKey, TTL_PANCAKE_CONV);
  if (cached) return cached;

  const lookbackDate = new Date(fromDate);
  lookbackDate.setDate(lookbackDate.getDate() - 90);
  const inserted_from = lookbackDate.toISOString().split('T')[0];

  const allConvs = [];
  let page = 1;
  while (true) {
    const url = `https://pancake.biz/api/v1/pages/${pageId}/conversations` +
      `?inserted_from=${inserted_from}&inserted_to=${toDate}` +
      `&limit=500&page=${page}&access_token=${pageToken(pageId)}`;
    try {
      const res = await fetchJson(url);
      const convs = Array.isArray(res) ? res : (res.conversations || res.data || []);
      allConvs.push(...convs);
      if (convs.length < 500) break;
      page++;
      if (page > 20) break;
    } catch(e) {
      console.warn(`Pancake conv fetch error ${pageId}: ${e.message}`);
      break;
    }
  }

  cacheSet(pancakeConvCache, cacheKey, allConvs);
  return allConvs;
}

async function fetchPancakeTagDefs(pageId) {
  const cacheKey = `tagdefs_${pageId}`;
  const cached = cacheGet(pancakeConvCache, cacheKey, 30 * 60 * 1000);
  if (cached) return cached;
  try {
    const res = await fetchJson(`https://pancake.biz/api/v1/pages/${pageId}/settings?access_token=${pageToken(pageId)}`);
    const tags = (res.settings && res.settings.tags) ? res.settings.tags : [];
    const map = {};
    for (const t of tags) { map[t.id] = (t.text || '').toUpperCase().trim(); }
    cacheSet(pancakeConvCache, cacheKey, map);
    return map;
  } catch(e) {
    return {};
  }
}

const AD_ACCOUNTS = [
  // --- CLEAR SIGHT ---
  { id: 'act_3948257548644609', name: 'CS New Page',   currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_MAIN },
  { id: 'act_2825452284312899', name: 'Clear Sight 1', currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_MAIN },
  { id: 'act_1264536714635179', name: 'Clear Sight 2', currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_MAIN },
  // --- CANPRO ---
  { id: 'act_1360519375937020', name: 'CanPro',        currency: 'PHP', product: 'CANPRO',      token: META_TOKEN_MAIN },
  // --- FIXORA (displayed as "Hearing Aid") ---
  { id: 'act_1783871125514527', name: 'Hearing Aid',   currency: 'PHP', product: 'FIXORA',      token: META_TOKEN_MAIN },
  // --- HEARWELL ---
  { id: 'act_971532679101983',  name: 'HearWell PH',   currency: 'PHP', product: 'HEARWELL',    token: META_TOKEN_MAIN },
];

// Currency conversion to PHP (update as needed)
const FX_TO_PHP = { 'PHP': 1, 'HKD': 7.3, 'USD': 56, 'SGD': 42 };

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const get = (u, r = 0) => {
      if (r > 10) return reject(new Error('Too many redirects'));
      const lib = u.startsWith('https') ? https : require('http');
      lib.get(u, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
        if ([301,302,303,307,308].includes(res.statusCode)) {
          const loc = res.headers.location;
          res.resume();
          return get(loc.startsWith('http') ? loc : new URL(loc, u).toString(), r + 1);
        }
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
      }).on('error', reject);
    };
    get(url);
  });
}

function fetchRaw(url) {
  return new Promise((resolve, reject) => {
    const get = (u, r = 0) => {
      if (r > 10) return reject(new Error('Too many redirects'));
      const lib = u.startsWith('https') ? https : require('http');
      lib.get(u, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/csv,*/*' } }, (res) => {
        if ([301,302,303,307,308].includes(res.statusCode)) {
          const loc = res.headers.location;
          res.resume();
          return get(loc.startsWith('http') ? loc : new URL(loc, u).toString(), r + 1);
        }
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve(data));
      }).on('error', reject);
    };
    get(url);
  });
}

function parseCSV(csv) {
  const lines = csv.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  // Normalize headers to lowercase so lookups are case-insensitive
  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = [];
    let cur = '', inQ = false;
    for (const ch of lines[i]) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cols.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
    cols.push(cur.trim());
    const row = {};
    headers.forEach((h, idx) => row[h] = (cols[idx] || '').replace(/"/g, '').trim());
    rows.push(row);
  }
  return rows;
}

function formatDate(d) {
  return d.getFullYear() + '-' +
    String(d.getMonth() + 1).padStart(2, '0') + '-' +
    String(d.getDate()).padStart(2, '0');
}


async function fetchPancakeSalesByDate() {
  const cached = cacheGet(pancakeCache, 'pancake', TTL_PANCAKE);
  if (cached) { console.log('✅ Pancake cache hit'); return cached; }
  try {
    const allCsvs = await Promise.all(PANCAKE_CSV_URLS.map(url => fetchRaw(url)));
    const csvRowCounts = allCsvs.map(csv => parseCSV(csv).length);
    const rows = allCsvs.flatMap(csv => parseCSV(csv));
    console.log(`📋 Pancake rows fetched: ${rows.length} (per sheet: ${csvRowCounts.join(', ')})`);

    const EXCLUDED_SELLERS = ['Marj Adana', 'Shan Chai Bertes'];

    const salesByDate = {};       // keyed by adId — for per-ad NDAP matching
    const clearSightByDate = {};  // ALL Clear Sight sales incl. no-ad rows
    const allSalesByDate   = {};  // ALL products total (Clear Sight + Haplunas + others)
    const productSalesByDate = {};// per-NDAP-product gross POS: { 'CANPRO': { date: {sales,orders} }, ... }

    let skipNoDate=0, skipCancel=0, skipNonClearSight=0, countedCS=0;

    for (const row of rows) {
      // All keys are lowercased by parseCSV — use lowercase lookups throughout
      const seller = (row['assigning seller'] || '').trim();
      const isExcludedSeller = seller && EXCLUDED_SELLERS.some(s => seller.toLowerCase() === s.toLowerCase());

      const price = parseFloat((row['unit price'] || '0').replace(/,/g, '')) || 0;
      const dateRaw = row['sales date'] || '';
      if (!dateRaw) { skipNoDate++; continue; }
      let dateStr;
      try {
        const d = new Date(dateRaw);
        if (isNaN(d.getTime())) { skipNoDate++; continue; }
        dateStr = formatDate(d);
      } catch(e) { skipNoDate++; continue; }

      const status = (row['status'] || '').trim().toUpperCase();
      const isCancelled = status.includes('CANCEL');

      const adId = (row['ads'] || '').trim();

      // Scan ALL column values — catches any column naming/casing variant
      const rowText      = Object.values(row).join(' ').toLowerCase();
      const isClearSight = rowText.includes('clear sight') || rowText.includes('clearsight');
      const isHaplunas   = rowText.includes('haplunas');
      const ndapProduct  = classifyNdapProduct(rowText); // 'CLEAR SIGHT'|'CANPRO'|'FIXORA'|'HEARWELL'|null

      // ── Per-ad sales (NDAP matching) — excluded sellers, cancelled, and Haplunas skipped ──
      if (!isExcludedSeller && !isCancelled && adId && !isHaplunas) {
        if (!salesByDate[dateStr]) salesByDate[dateStr] = {};
        if (!salesByDate[dateStr][adId]) salesByDate[dateStr][adId] = {
          sales: 0, orders: 0,
          delivered: 0, deliveredValue: 0,
          rts: 0, rtsValue: 0,
          shipped: 0, shippedValue: 0
        };
        salesByDate[dateStr][adId].sales  += price;
        salesByDate[dateStr][adId].orders += 1;
        if (status.includes('DELIVERED')) {
          salesByDate[dateStr][adId].delivered += 1;
          salesByDate[dateStr][adId].deliveredValue += price;
        } else if (status.includes('RTS') || status.includes('RETURN')) {
          salesByDate[dateStr][adId].rts += 1;
          salesByDate[dateStr][adId].rtsValue += price;
        } else if (status.includes('SHIP') || status.includes('TRANSIT')) {
          salesByDate[dateStr][adId].shipped += 1;
          salesByDate[dateStr][adId].shippedValue += price;
        }
      } else if (isCancelled) { skipCancel++; }

      // ── Clear Sight total — ALL orders incl. cancelled, to match mainfile ──
      if (isClearSight) {
        if (!clearSightByDate[dateStr]) clearSightByDate[dateStr] = { sales: 0, orders: 0 };
        clearSightByDate[dateStr].sales  += price;
        clearSightByDate[dateStr].orders += 1;
        countedCS++;
      } else {
        skipNonClearSight++;
      }

      // ── Per-NDAP-product gross POS total (incl. no-ad rows & cancelled, to match mainfile) ──
      if (ndapProduct) {
        if (!productSalesByDate[ndapProduct]) productSalesByDate[ndapProduct] = {};
        if (!productSalesByDate[ndapProduct][dateStr]) productSalesByDate[ndapProduct][dateStr] = { sales: 0, orders: 0 };
        productSalesByDate[ndapProduct][dateStr].sales  += price;
        productSalesByDate[ndapProduct][dateStr].orders += 1;
      }

      // ── All-products total (incl. Haplunas, all sellers, cancelled) ──
      if (!allSalesByDate[dateStr]) allSalesByDate[dateStr] = { sales: 0, orders: 0 };
      allSalesByDate[dateStr].sales  += price;
      allSalesByDate[dateStr].orders += 1;
    }

    console.log(`📊 Pancake parse: ${countedCS} Clear Sight rows counted, ${skipCancel} cancelled, ${skipNoDate} no-date, ${skipNonClearSight} non-CS`);
    console.log('📅 clearSightByDate totals:', Object.entries(clearSightByDate).map(([d,v])=>`${d}:₱${v.sales.toFixed(0)}(${v.orders})`).join(', '));

    const result = { salesByDate, clearSightByDate, allSalesByDate, productSalesByDate };
    cacheSet(pancakeCache, 'pancake', result);
    return result;
  } catch(e) {
    console.error('Pancake CSV error:', e.message);
    return { salesByDate: {}, clearSightByDate: {}, allSalesByDate: {}, productSalesByDate: {} };
  }
}

// Classify a POS row into an NDAP product label (must match AD_ACCOUNTS product values)
function classifyNdapProduct(rowText) {
  if (rowText.includes('clear sight') || rowText.includes('clearsight')) return 'CLEAR SIGHT';
  if (rowText.includes('canpro') || rowText.includes('can pro') || rowText.includes('canro')) return 'CANPRO';
  if (rowText.includes('fixora')) return 'FIXORA';
  if (rowText.includes('hearwell') || rowText.includes('hear well')) return 'HEARWELL';
  // Legacy: older POS rows labelled "Hearing Aid" belong to the Fixora hearing-aid product
  if (rowText.includes('hearing aid') || rowText.includes('hearingaid') || rowText.includes('audicure')) return 'FIXORA';
  return null;
}

async function fetchAccountInsights(account, date) {
  const metaKey = `${account.id}_${date}`;
  const cached = cacheGet(metaInsightsCache, metaKey, TTL_META);
  if (cached) { console.log(`✅ Meta cache hit: ${metaKey}`); return cached; }
  const token = account.token || '';
  const fxRate = FX_TO_PHP[account.currency || 'PHP'] || 1;

  // Fetch all pages of ad-level insights
  const allRows = [];
  let nextUrl = `https://graph.facebook.com/v19.0/${account.id}/insights` +
    `?fields=ad_id,ad_name,campaign_id,campaign_name,spend,impressions,clicks,frequency,cost_per_action_type,actions` +
    `&level=ad&time_range={"since":"${date}","until":"${date}"}` +
    `&limit=500&access_token=${token}`;

  try {
    while (nextUrl) {
      const res = await fetchJson(nextUrl);
      if (res.error) {
        console.warn(`⚠️  Meta API error for ${account.name} (${account.id}): ${res.error.message}`);
        break;
      }
      const page = res.data || [];
      allRows.push(...page);
      nextUrl = res.paging && res.paging.next ? res.paging.next : null;
    }

    // If no ad-level data, try account-level fallback
    if (allRows.length === 0) {
      const fallbackUrl = `https://graph.facebook.com/v19.0/${account.id}/insights` +
        `?fields=spend,impressions,clicks` +
        `&level=account&time_range={"since":"${date}","until":"${date}"}` +
        `&access_token=${token}`;
      const fallback = await fetchJson(fallbackUrl);
      if (fallback.data && fallback.data.length > 0 && parseFloat(fallback.data[0].spend || 0) > 0) {
        const f = fallback.data[0];
        const fallbackResult = [{
          accountName: account.name,
          adId: `${account.id}_fallback`,
          adName: '(Historical — campaign detail unavailable)',
          campaignId: `${account.id}_fallback`,
          campaignName: `${account.name} — Total Spend (historical)`,
          spend: parseFloat(f.spend || 0),
          impressions: parseInt(f.impressions || 0),
          clicks: parseInt(f.clicks || 0),
          isFallback: true
        }];
        cacheSet(metaInsightsCache, metaKey, fallbackResult);
        return fallbackResult;
      }
      cacheSet(metaInsightsCache, metaKey, []);
      return [];
    }

    const mapped = allRows.map(r => {
      const msgAction = (r.actions || []).find(a =>
        a.action_type === 'onsite_conversion.total_messaging_connection' ||
        a.action_type === 'onsite_conversion.messaging_conversation_started_7d'
      );
      const messagesStarted = msgAction ? parseInt(msgAction.value) : 0;
      const costPerMessage = messagesStarted > 0 ? parseFloat(r.spend || 0) * fxRate / messagesStarted : 0;

      return {
        accountName: account.name,
        product: account.product || '',
        adId: r.ad_id, adName: r.ad_name,
        campaignId: r.campaign_id, campaignName: r.campaign_name,
        spend: parseFloat(r.spend || 0) * fxRate,
        impressions: parseInt(r.impressions || 0),
        clicks: parseInt(r.clicks || 0),
        frequency: parseFloat(r.frequency || 0),
        costPerMessage,
        messagesStarted
      };
    });
    cacheSet(metaInsightsCache, metaKey, mapped);
    return mapped;
  } catch(e) {
    console.error(`❌ fetchAccountInsights failed for ${account.name} (${account.id}): ${e.message}`);
    return [];
  }
}

async function fetchBudgetsForAccount(account) {
  const cached = cacheGet(budgetCache, account.id, TTL_BUDGET);
  if (cached) { console.log(`✅ Budget cache hit: ${account.id}`); return cached; }
  const fxRate = FX_TO_PHP[account.currency || 'PHP'] || 1;
  const result = {};
  try {
    let nextUrl = `https://graph.facebook.com/v19.0/${account.id}/ads` +
      `?fields=id,adset{daily_budget,campaign{daily_budget}}&limit=100&access_token=${account.token || ''}`;
    while (nextUrl) {
      const res = await fetchJson(nextUrl);
      if (res.error) { console.warn(`⚠️  Budget fetch error for ${account.name}: ${res.error.message}`); break; }
      for (const ad of (res.data || [])) {
        let budget = 0;
        if (ad.adset) {
          if (ad.adset.daily_budget && parseInt(ad.adset.daily_budget) > 0)
            budget = parseInt(ad.adset.daily_budget) / 100 * fxRate;
          else if (ad.adset.campaign && ad.adset.campaign.daily_budget)
            budget = parseInt(ad.adset.campaign.daily_budget) / 100 * fxRate;
        }
        result[ad.id] = budget;
      }
      nextUrl = res.paging && res.paging.next ? res.paging.next : null;
    }
  } catch(e) { console.error(`❌ fetchBudgetsForAccount failed for ${account.name}: ${e.message}`); }
  cacheSet(budgetCache, account.id, result);
  return result;
}

async function fetchAllBudgets() {
  const maps = await Promise.all(AD_ACCOUNTS.map(acc => fetchBudgetsForAccount(acc)));
  return Object.assign({}, ...maps);
}

// Fetch all ACTIVE ads per account (so we never miss ads with 0 spend on a date)
const activeAdsCache = new Map();
const TTL_ACTIVE_ADS = 30 * 60 * 1000;

async function fetchActiveAdsForAccount(account) {
  const cached = cacheGet(activeAdsCache, account.id, TTL_ACTIVE_ADS);
  if (cached) { console.log(`✅ Active ads cache hit: ${account.id}`); return cached; }
  const token = account.token || '';
  const result = [];
  try {
    let nextUrl = `https://graph.facebook.com/v19.0/${account.id}/ads` +
      `?fields=id,name,campaign_id,campaign{name}` +
      `&filtering=[{"field":"ad.effective_status","operator":"IN","value":["ACTIVE","WITH_ISSUES","IN_PROCESS","PENDING_REVIEW","PREAPPROVED"]}]` +
      `&limit=100&access_token=${token}`;
    while (nextUrl) {
      const res = await fetchJson(nextUrl);
      if (res.error) { console.warn(`⚠️  Active ads fetch error for ${account.name}: ${res.error.message}`); break; }
      for (const ad of (res.data || [])) {
        result.push({
          adId: ad.id,
          adName: ad.name,
          campaignId: ad.campaign_id,
          campaignName: ad.campaign ? ad.campaign.name : '',
          accountName: account.name,
          product: account.product || '',
        });
      }
      nextUrl = res.paging && res.paging.next ? res.paging.next : null;
    }
  } catch(e) { console.error(`❌ fetchActiveAds failed for ${account.name}: ${e.message}`); }
  cacheSet(activeAdsCache, account.id, result);
  return result;
}

async function fetchAllActiveAds() {
  const results = [];
  for (const acc of AD_ACCOUNTS) {
    const ads = await fetchActiveAdsForAccount(acc);
    results.push(...ads);
  }
  return results;
}

// ── GRANULAR IN-MEMORY CACHE ──
const metaInsightsCache = new Map(); // key: accountId_date       TTL: 30 min
const pancakeCache       = new Map(); // key: 'pancake'            TTL: 30 min
const budgetCache        = new Map(); // key: accountId            TTL: 1 hour

const TTL_META    = 30 * 60 * 1000;
const TTL_PANCAKE = 30 * 60 * 1000;
const TTL_BUDGET  =  1 * 60 * 60 * 1000;

function cacheGet(map, key, ttl) {
  const entry = map.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > ttl) { map.delete(key); return null; }
  return entry.data;
}
function cacheSet(map, key, data) {
  map.set(key, { data, ts: Date.now() });
}

// Public assets
app.get('/logo.svg', (_req, res) => res.sendFile(path.join(__dirname, 'logo.svg')));
app.get('/mlc-logo.png', (_req, res) => res.sendFile(path.join(__dirname, 'mlc-logo.png')));
app.get('/favicon.ico', (_req, res) => res.sendFile(path.join(__dirname, 'mlc-logo.png')));

// Auth routes
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'login.html')));
app.get('/auth/google', passport.authenticate('google', { scope: ['profile', 'email'] }));
app.get('/auth/google/callback',
  (req, res, next) => {
    passport.authenticate('google', (err, user, info) => {
      if (err) {
        console.error('OAuth callback error:', err.message || err);
        return res.redirect('/login?error=oauth_error');
      }
      if (!user) {
        console.warn('OAuth: unauthorized user', info);
        return res.redirect('/login?error=unauthorized');
      }
      req.logIn(user, (loginErr) => {
        if (loginErr) {
          console.error('Login session error:', loginErr.message || loginErr);
          return res.redirect('/login?error=session_error');
        }
        return res.redirect('/home');
      });
    })(req, res, next);
  }
);
app.get('/logout', (req, res) => { req.logout(() => res.redirect('/login')); });
app.get('/api/me', requireAuth, (req, res) => res.json(req.user));

// ── ADMIN: user management (admins only) ──
const VALID_ROLES = ['FSA', 'Logistics', 'Advertiser', 'Admin'];

app.get('/api/users', requireAdmin, (_req, res) => {
  const seed = AUTHORIZED_USERS.map(u => ({ email: u.email, name: u.name, role: u.role, source: 'system' }));
  const db   = dbUsers.map(u => ({ email: u.email, name: u.name, role: u.role, source: 'db' }));
  res.json([...seed, ...db]);
});

app.post('/api/users', requireAdmin, async (req, res) => {
  try {
    const email = normEmail(req.body.email);
    const name  = (req.body.name || '').trim();
    const role  = (req.body.role || '').trim();
    if (!email || !name || !role) return res.status(400).json({ error: 'Kumpletuhin ang lahat ng fields.' });
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return res.status(400).json({ error: 'Invalid email format.' });
    if (!VALID_ROLES.some(r => r.toLowerCase() === role.toLowerCase())) return res.status(400).json({ error: 'Invalid role.' });
    if (findUser(email)) return res.status(409).json({ error: 'May account na ang email na ito.' });
    await pool.query('INSERT INTO app_users (email, name, role, created_by) VALUES ($1,$2,$3,$4)', [email, name, role, req.user.email]);
    await loadDbUsers();
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/users/:email', requireAdmin, async (req, res) => {
  try {
    const email = normEmail(req.params.email);
    if (SEED_EMAILS.has(email)) return res.status(400).json({ error: 'Hindi maaaring alisin ang system user.' });
    if (email === normEmail(req.user.email)) return res.status(400).json({ error: 'Hindi mo maaaring alisin ang sarili mo.' });
    await pool.query('DELETE FROM app_users WHERE email=$1', [email]);
    await loadDbUsers();
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/flush-cache', requireAuth, (req, res) => {
  pancakeCache.clear(); metaInsightsCache.clear(); budgetCache.clear(); activeAdsCache.clear(); pancakeConvCache.clear();
  console.log('🗑️ All caches flushed by', req.user.email);
  res.json({ ok: true, message: 'All caches cleared — next load will fetch fresh data' });
});

app.get('/api/debug-pancake', requireAuth, async (req, res) => {
  pancakeCache.clear();
  const data = await fetchPancakeSalesByDate();
  res.json({ clearSightByDate: data.clearSightByDate });
});

// Debug: inspect raw pages.fm v2 response structure (1 page only)
app.get('/api/debug-pancake-v2', requireAuth, async (req, res) => {
  const pageId = req.query.pageId || '183224001550935';
  const token  = PANCAKE_PAGE_TOKENS[pageId];
  if (!token) return res.status(400).json({ error: 'No token for pageId' });
  const date  = req.query.date || new Date().toISOString().split('T')[0];
  const since = Math.floor(new Date(date + 'T00:00:00+08:00').getTime() / 1000);
  const until = Math.floor(new Date(date + 'T23:59:59+08:00').getTime() / 1000);
  const lastId = req.query.lastId || null;
  let url = `https://pages.fm/api/public_api/v2/pages/${pageId}/conversations` +
    `?page_access_token=${token}&type=INBOX&since=${since}&until=${until}&limit=60`;
  if (lastId) url += `&last_conversation_id=${lastId}`;
  try {
    const raw = await fetchJson(url);
    const convs = raw.conversations || raw.data || [];
    res.json({
      topLevelKeys: Object.keys(raw),
      has_more: raw.has_more,
      total: raw.total,
      convCount: convs.length,
      firstConvKeys: convs[0] ? Object.keys(convs[0]) : [],
      firstConvId: convs[0]?.id,
      firstConvAssignUsers: convs[0]?.current_assign_users,
      lastConvId: convs[convs.length-1]?.id,
      sampleAssignees: convs.slice(0,5).map(c=>({ id:c.id, assignUsers:c.current_assign_users })),
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Action log routes
app.post('/api/action-log', requireAuth, async (req, res) => {
  const { campaign_key, campaign_name, ad_account, date, recommendation, action_taken, new_budget, notes } = req.body;
  if (!campaign_key || !action_taken) return res.status(400).json({ error: 'Missing fields' });
  try {
    const result = await pool.query(
      `INSERT INTO action_logs (campaign_key, campaign_name, ad_account, date, recommendation, action_taken, new_budget, notes, done_by_email, done_by_name)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
      [campaign_key, campaign_name, ad_account, date, recommendation, action_taken, new_budget||null, notes||null, req.user.email, req.user.name]
    );
    res.json({ id: result.rows[0].id, success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/action-logs', requireAuth, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM action_logs ORDER BY created_at DESC LIMIT 200');
    res.json(result.rows);
  } catch(e) { res.json([]); }
});

app.get('/api/action-logs/:campaign_key', requireAuth, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM action_logs WHERE campaign_key=$1 ORDER BY created_at DESC', [req.params.campaign_key]);
    res.json(result.rows);
  } catch(e) { res.json([]); }
});

app.get('/api/creatives', requireAuth, async (_req, res) => {
  try {
    const result = await pool.query('SELECT * FROM creatives ORDER BY created_at DESC');
    res.json(result.rows);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/creatives', requireAuth, async (req, res) => {
  const { campaign_key, link, title } = req.body || {};
  if (!campaign_key || !link) return res.status(400).json({ error: 'Missing fields' });
  if (!String(link).includes('.com')) return res.status(400).json({ error: 'Link must contain .com' });
  try {
    const result = await pool.query(
      `INSERT INTO creatives (campaign_key, link, title, submitted_by_email, submitted_by_name)
       VALUES ($1,$2,$3,$4,$5)
       RETURNING *`,
      [campaign_key, String(link).trim(), title ? String(title).trim() : null, req.user.email, req.user.name]
    );
    res.json({ success: true, creative: result.rows[0] });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Manual budget routes
app.post('/api/budget', requireAuth, async (req, res) => {
  const { page_key, date, budget, locked } = req.body;
  if (!page_key || !date) return res.status(400).json({ error: 'Missing fields' });
  try {
    await pool.query(
      `INSERT INTO page_budgets (page_key, date, budget, locked, set_by_email, set_by_name)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (page_key, date) DO UPDATE SET budget=$3, locked=$4, set_by_email=$5, set_by_name=$6`,
      [page_key, date, budget||null, locked||false, req.user.email, req.user.name]
    );
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/budgets/:date', requireAuth, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM page_budgets WHERE date=$1', [req.params.date]);
    const map = {};
    result.rows.forEach(r => map[r.page_key] = { budget: parseFloat(r.budget||0), locked: r.locked });
    res.json(map);
  } catch(e) { res.json({}); }
});

// Report hub + protected pages
app.get('/home',              requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'home.html')));
app.get('/ad-spend',          requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'ad-spend.html')));
app.get('/logistics',         requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'logistics.html')));
app.get('/admin',             requireAdmin, (_req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/roas-report',       requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'roas-report.html')));
app.get('/income-statement',  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'income-statement.html')));
app.get('/aov-cvr-report',    requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'aov-cvr-report.html')));
app.get('/sales-report',      requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'sales-report.html')));
app.get('/',                  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/ndap', requireAuth, async (req, res) => {
  try {
    const fromDate = req.query.from || null;
    const toDate = req.query.to || req.query.endDate || new Date().toISOString().split('T')[0];
    const days = parseInt(req.query.days || 3);

    const dates = [];
    if (fromDate) {
      const start = new Date(fromDate + 'T00:00:00Z');
      const end = new Date(toDate + 'T00:00:00Z');
      for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
        dates.push(d.toISOString().split('T')[0]);
      }
    } else {
      for (let i = days - 1; i >= 0; i--) {
        const parts = toDate.split('-').map(Number);
        const d = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2]));
        d.setUTCDate(d.getUTCDate() - i);
        dates.push(d.toISOString().split('T')[0]);
      }
    }

    const [pancakeData, budgetMap, activeAdsList] = await Promise.all([
      fetchPancakeSalesByDate(), fetchAllBudgets(), fetchAllActiveAds()
    ]);
    const { salesByDate, clearSightByDate, allSalesByDate, productSalesByDate } = pancakeData;

    // Build adMap keyed by adId — all 14 active ads pre-seeded so none get dropped
    const adMap = {};
    for (const ad of activeAdsList) {
      adMap[ad.adId] = {
        campaignId: ad.campaignId, campaignName: ad.campaignName,
        adId: ad.adId, adName: ad.adName,
        accountName: ad.accountName,
        product: ad.product || '',
        budget: budgetMap[ad.adId] || 0,
        dates: {}
      };
    }

    // Sequential per-date fetching with staggered account calls to avoid Meta automation flags
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    const insightsByDate = {};
    for (const date of dates) {
      const allRows = [];
      for (const acc of AD_ACCOUNTS) {
        const rows = await fetchAccountInsights(acc, date);
        allRows.push(...rows);
        await sleep(300);
      }
      insightsByDate[date] = allRows;
      await sleep(500);
    }

    // Merge insights into adMap — one row per ad, grouped by adId
    // Also add ads that had spend but are no longer active (paused/stopped after the date)
    for (const date of dates) {
      for (const row of (insightsByDate[date] || [])) {
        if (!adMap[row.adId]) {
          // Ad not in activeAdsList (paused/stopped) but had spend — add dynamically
          adMap[row.adId] = {
            campaignId: row.campaignId, campaignName: row.campaignName,
            adId: row.adId, adName: row.adName,
            accountName: row.accountName,
            product: row.product || '',
            budget: budgetMap[row.adId] || 0,
            dates: {}
          };
        }
        const sales = salesByDate[date]?.[row.adId] || { sales: 0, orders: 0, delivered: 0, deliveredValue: 0, rts: 0, rtsValue: 0, shipped: 0, shippedValue: 0 };

        if (!adMap[row.adId].dates[date]) {
          adMap[row.adId].dates[date] = {
            spend: 0, grossSales: 0, orders: 0,
            impressions: 0, clicks: 0, frequency: 0,
            costPerMessage: 0, messagesStarted: 0,
            delivered: 0, deliveredValue: 0,
            rts: 0, rtsValue: 0, shipped: 0, shippedValue: 0
          };
        }
        const d = adMap[row.adId].dates[date];
        d.spend          += row.spend;
        d.grossSales     += sales.sales || 0;
        d.orders         += sales.orders || 0;
        d.impressions    += row.impressions;
        d.clicks         += row.clicks;
        d.messagesStarted += row.messagesStarted || 0;
        if (row.frequency > d.frequency) d.frequency = row.frequency;
        d.costPerMessage  += row.costPerMessage || 0;
        d.delivered      += sales.delivered || 0;
        d.deliveredValue += sales.deliveredValue || 0;
        d.rts            += sales.rts || 0;
        d.rtsValue       += sales.rtsValue || 0;
        d.shipped        += sales.shipped || 0;
        d.shippedValue   += sales.shippedValue || 0;
      }
    }

    // Calculate derived metrics
    for (const ad of Object.values(adMap)) {
      for (const d of Object.values(ad.dates)) {
        d.roas    = d.spend > 0 ? d.grossSales / d.spend : 0;
        d.cpp     = d.orders > 0 ? d.spend / d.orders : 0;
        d.cpm     = d.messagesStarted > 0 ? d.spend / d.messagesStarted : 0;
        d.delRate = (d.delivered + d.rts) > 0 ? (d.delivered / (d.delivered + d.rts) * 100) : null;
      }
    }

    const campaigns = Object.values(adMap).map(c => {
      let ts = 0, tsp = 0, to = 0, tDel = 0, tRts = 0, tShip = 0, tDelVal = 0, tRtsVal = 0, maxFreq = 0;
      for (const d of Object.values(c.dates)) {
        ts += d.grossSales; tsp += d.spend; to += d.orders;
        tDel += d.delivered || 0; tRts += d.rts || 0; tShip += d.shipped || 0;
        tDelVal += d.deliveredValue || 0; tRtsVal += d.rtsValue || 0;
        if ((d.frequency || 0) > maxFreq) maxFreq = d.frequency;
      }
      const overallDelRate = (tDel + tRts) > 0 ? (tDel / (tDel + tRts) * 100) : null;
      return { ...c, totalSales: ts, totalSpend: tsp, totalOrders: to, maxFrequency: maxFreq,
        totalRoas: tsp > 0 ? ts / tsp : 0,
        totalDelivered: tDel, totalRts: tRts, totalShipped: tShip,
        totalDeliveredValue: tDelVal, totalRtsValue: tRtsVal, overallDelRate };
    });

    // Only show ads that actually spent in the selected date range
    const activeCampaigns = campaigns.filter(c => c.totalSpend > 0);

    // Mirror the manual NDAP order; unknown/new ads go to the end
    const AD_SORT_ORDER = [
      '120239574138660721',
      '120239500552690721',
      '120245022766230721',
      '120245023075110721',
      '120242885178630617',
      '120242885220370617',
      '120241663175620617',
      '120242025285400617',
      '120243655307940617',
      '120243392946150617',
      '120243392500850617',
      '120243392998560617',
      '120246106321330721',
      '120246106300370721',
      '120246106033340721',
    ];
    activeCampaigns.sort((a, b) => {
      const ia = AD_SORT_ORDER.indexOf(a.adId);
      const ib = AD_SORT_ORDER.indexOf(b.adId);
      if (ia === -1 && ib === -1) return a.adName.localeCompare(b.adName);
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    });

    const result = { dates, campaigns: activeCampaigns, clearSightByDate, allSalesByDate, productSalesByDate };
    res.json(result);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Inquiries (messaging conversations started) split by PHT time-of-day window.
// morning = 6 AM–3 PM, evening = 3 PM–12 AM. Uses Meta hourly breakdown
// (advertiser time zone = Asia/Manila for these accounts).
app.get('/api/inquiries-hourly', requireAuth, async (req, res) => {
  try {
    const from = req.query.from;
    const to   = req.query.to || from;
    if (!from) return res.status(400).json({ error: 'from required' });
    const cacheKey = `inq_${from}_${to}`;
    const cached = cacheGet(metaInsightsCache, cacheKey, TTL_META);
    if (cached) return res.json(cached);

    let morning = 0, evening = 0; // 6AM–3PM, 3PM–12AM (all products)
    const byProduct = {};         // { 'CLEAR SIGHT': { morning, evening }, ... }
    for (const acc of AD_ACCOUNTS) {
      const prod = acc.product || 'OTHER';
      if (!byProduct[prod]) byProduct[prod] = { morning: 0, evening: 0 };
      let nextUrl = `https://graph.facebook.com/v19.0/${acc.id}/insights` +
        `?level=account&fields=actions&breakdowns=hourly_stats_aggregated_by_advertiser_time_zone` +
        `&time_range={"since":"${from}","until":"${to}"}&limit=500&access_token=${acc.token || ''}`;
      try {
        while (nextUrl) {
          const r = await fetchJson(nextUrl);
          if (r.error) { console.warn(`⚠️ inquiries-hourly ${acc.name}: ${r.error.message}`); break; }
          for (const row of (r.data || [])) {
            const hourStr = row.hourly_stats_aggregated_by_advertiser_time_zone || '';
            const hr = parseInt(hourStr.slice(0, 2));
            const msgAct = (row.actions || []).find(a =>
              a.action_type === 'onsite_conversion.total_messaging_connection' ||
              a.action_type === 'onsite_conversion.messaging_conversation_started_7d'
            );
            const v = msgAct ? parseInt(msgAct.value) : 0;
            if (hr >= 6 && hr < 15)        { morning += v; byProduct[prod].morning += v; }
            else if (hr >= 15 && hr <= 23) { evening += v; byProduct[prod].evening += v; }
          }
          nextUrl = r.paging && r.paging.next ? r.paging.next : null;
        }
      } catch(e) { console.warn(`⚠️ inquiries-hourly fetch ${acc.name}: ${e.message}`); }
    }
    const result = { from, to, morning, evening, total: morning + evening, byProduct };
    cacheSet(metaInsightsCache, cacheKey, result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── LOGISTICS: delivery / RTS analytics from the POS export ──
const logisticsCache = new Map();
// basis: 'order' = filter by Sales Date (cohort); 'delivery' = delivered rows filtered by Delivered Date
async function fetchLogistics(fromDate, toDate, basis = 'order') {
  const key = `log_${basis}_${fromDate}_${toDate}`;
  const cached = cacheGet(logisticsCache, key, TTL_PANCAKE);
  if (cached) return cached;

  const allCsvs = await Promise.all(PANCAKE_CSV_URLS.map(url => fetchRaw(url)));
  const rows = allCsvs.flatMap(csv => parseCSV(csv));
  const from = new Date(fromDate + 'T00:00:00Z');
  const to   = new Date(toDate   + 'T23:59:59Z');
  const PROD_LABEL = { 'CLEAR SIGHT':'Clear Sight', 'CANPRO':'CanPro', 'FIXORA':'Hearing Aid', 'HEARWELL':'HearWell' };

  const blank = () => ({ delivered:0, returned:0, shipped:0, cancelled:0, deliveredValue:0, returnedValue:0, shippedValue:0 });
  const bump = (o, bucket, price) => {
    if (bucket === 'delivered')      { o.delivered++; o.deliveredValue += price; }
    else if (bucket === 'returned')  { o.returned++;  o.returnedValue  += price; }
    else if (bucket === 'cancelled') { o.cancelled++; }
    else                             { o.shipped++;   o.shippedValue   += price; }
  };
  const totals = blank(), byProduct = {}, byRegion = {};

  for (const row of rows) {
    const status = (row['status'] || '').toLowerCase();
    let bucket;
    if (status.includes('deliver'))    bucket = 'delivered';
    else if (status.includes('return')) bucket = 'returned';
    else if (status.includes('cancel')) bucket = 'cancelled';
    else                                bucket = 'shipped'; // shipped / in-transit / packaging / waiting

    // Choose the date to filter by. In 'delivery' basis, delivered rows use the
    // actual Delivered Date; everything else falls back to Sales Date (no RTS date in sheet).
    let dateRaw = row['sales date'] || '';
    if (basis === 'delivery' && bucket === 'delivered' && (row['delivered date'] || '').trim()) {
      dateRaw = row['delivered date'];
    }
    if (!dateRaw) continue;
    let d; try { d = new Date(dateRaw); if (isNaN(d.getTime())) continue; } catch(e) { continue; }
    if (d < from || d > to) continue;

    const price  = parseFloat((row['unit price'] || '0').replace(/,/g,'')) || 0;
    const prodKey = classifyNdapProduct(Object.values(row).join(' ').toLowerCase());
    const product = prodKey ? (PROD_LABEL[prodKey] || prodKey) : ((row['product name'] || 'Other').trim() || 'Other');
    const region  = (row['by region'] || '').trim() || 'Unknown';

    bump(totals, bucket, price);
    if (!byProduct[product]) byProduct[product] = blank();
    bump(byProduct[product], bucket, price);
    if (!byRegion[region]) byRegion[region] = blank();
    bump(byRegion[region], bucket, price);
  }

  const withRates = (o, name) => {
    const resolved = o.delivered + o.returned; // completed outcomes
    return { name, ...o,
      orders: o.delivered + o.returned + o.shipped + o.cancelled,
      grossValue: o.deliveredValue + o.returnedValue + o.shippedValue,
      deliveryRate: resolved > 0 ? o.delivered / resolved * 100 : null,
      rtsRate:      resolved > 0 ? o.returned  / resolved * 100 : null };
  };
  const result = {
    from: fromDate, to: toDate,
    totals: withRates(totals, 'All'),
    byProduct: Object.entries(byProduct).map(([n,o]) => withRates(o,n)).sort((a,b)=>b.orders-a.orders),
    byRegion:  Object.entries(byRegion).map(([n,o]) => withRates(o,n)).sort((a,b)=>b.orders-a.orders),
  };
  cacheSet(logisticsCache, key, result);
  return result;
}

app.get('/api/logistics', requireAuth, async (req, res) => {
  try {
    const from  = req.query.from || new Date().toISOString().split('T')[0];
    const to    = req.query.to   || from;
    const basis = req.query.basis === 'delivery' ? 'delivery' : 'order';
    res.json(await fetchLogistics(from, to, basis));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── LIVE DELIVERY STATUS from Pancake POS API (real-time order statuses) ──
// Pancake status codes: 0 new · 1 submitted · 2 shipped · 3 delivered · 4 returning
//                       5 returned · 6 canceled · 7 removed · 8 packing · 11 waiting
const deliveryStatusCache = new Map();
async function fetchDeliveryStatus(fromDate, toDate) {
  const key = `dlv_${fromDate}_${toDate}`;
  const cached = cacheGet(deliveryStatusCache, key, 5 * 60 * 1000);
  if (cached) return cached;

  const since = Math.floor(new Date(fromDate + 'T00:00:00+08:00').getTime() / 1000);
  const until = Math.floor(new Date(toDate   + 'T23:59:59+08:00').getTime() / 1000);
  const url = `https://pos.pages.fm/api/v1/shops/${PANCAKE_POS_SHOP}/orders` +
    `?api_key=${PANCAKE_POS_KEY}&page_size=1&startDateTime=${since}&endDateTime=${until}`;
  const res = await fetchJson(url);
  const buckets = (((res.aggs || {}).status) || {}).buckets || [];
  const cnt = {}; for (const b of buckets) cnt[String(b.key)] = b.doc_count;
  const g = k => cnt[String(k)] || 0;

  const result = {
    from: fromDate, to: toDate,
    total:          res.total_entries || 0,
    toShip:         g(0) + g(1) + g(8) + g(11), // new / submitted / packing / waiting
    outForDelivery: g(2),                        // shipped — on the way
    delivered:      g(3),
    rts:            g(4) + g(5),                  // returning + returned
    cancelled:      g(6) + g(7),                  // canceled + removed
  };
  cacheSet(deliveryStatusCache, key, result);
  return result;
}

app.get('/api/delivery-status', requireAuth, async (req, res) => {
  try {
    const from = req.query.from || new Date().toISOString().split('T')[0];
    const to   = req.query.to   || from;
    res.json(await fetchDeliveryStatus(from, to));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── OUT-FOR-DELIVERY WATCHLIST — live list of parcels currently on the way + COD at risk ──
async function fetchOutForDelivery() {
  const cached = cacheGet(deliveryStatusCache, 'ofd', 5 * 60 * 1000);
  if (cached) return cached;

  const all = [];
  let page = 1, totalPages = 1;
  while (page <= totalPages && page <= 30) {
    const url = `https://pos.pages.fm/api/v1/shops/${PANCAKE_POS_SHOP}/orders` +
      `?api_key=${PANCAKE_POS_KEY}&page_size=100&page_number=${page}&status=2`;
    const res = await fetchJson(url);
    if (res.error || res.success === false) break;
    totalPages = res.total_pages || 1;
    const data = res.data || [];
    all.push(...data);
    if (data.length < 100) break;
    page++;
  }

  const now = Date.now();
  const orders = all.map(o => {
    const p = o.partner || {};
    const addr = o.shipping_address || {};
    const sentRaw = o.time_send_partner || p.picked_up_at || p.first_delivery_at || o.inserted_at;
    const sentMs = sentRaw ? new Date(sentRaw).getTime() : null;
    const days = sentMs ? Math.max(0, Math.floor((now - sentMs) / 86400000)) : null;
    return {
      id:            o.system_id || o.id,
      name:          o.bill_full_name || (o.customer && o.customer.name) || '—',
      phone:         o.bill_phone_number || (o.customer && o.customer.phone_number) || '',
      courier:       p.partner_name || '—',
      partnerStatus: p.partner_status || '',
      cod:           Number(o.money_to_collect || o.cod || 0),
      attempts:      p.count_of_delivery || 0,
      days,
      region:        addr.province_name || addr.province || addr.state || '—',
      tracking:      p.extend_code || '',
    };
  });
  // Most at-risk first: undeliverable, then more attempts, then longer on the way
  orders.sort((a, b) =>
    (Number(b.partnerStatus === 'undeliverable') - Number(a.partnerStatus === 'undeliverable')) ||
    (b.attempts - a.attempts) || ((b.days || 0) - (a.days || 0)));

  const result = {
    count: orders.length,
    totalCod: orders.reduce((s, o) => s + o.cod, 0),
    atRisk: orders.filter(o => o.partnerStatus === 'undeliverable' || o.attempts >= 2 || (o.days || 0) >= 7).length,
    orders,
  };
  cacheSet(deliveryStatusCache, 'ofd', result);
  return result;
}

app.get('/api/out-for-delivery', requireAuth, async (_req, res) => {
  try {
    res.json(await fetchOutForDelivery());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/aov-cvr', requireAuth, async (req, res) => {
  try {
    const fromDate = req.query.from || new Date().toISOString().split('T')[0];
    const toDate   = req.query.to   || fromDate;

    // Step 1 — Fetch POS orders from all monthly MLC mainfiles and combine
    const csvChunks = await Promise.all(MLC_MAINFILE_CSV_URLS.map(url => fetchRaw(url)));
    const rows = csvChunks.flatMap(csv => parseCSV(csv));

    const fromDt = new Date(fromDate + 'T00:00:00Z');
    const toDt   = new Date(toDate   + 'T23:59:59Z');

    const orders = [];
    const pageIdsNeeded = new Set();

    for (const row of rows) {
      const sellerRaw = (row['assigning seller'] || '').trim();
      if (!sellerRaw) continue;

      const productName = (row['product name'] || '').toLowerCase();
      const pageName    = (row['facebook page'] || '').toLowerCase();
      const isClearSight = productName.includes('clear sight') ||
                           productName.includes('clear vision') ||
                           pageName.includes('clear sight') ||
                           pageName.includes('clear vision') ||
                           pageName.includes('eyecare hub');
      if (!isClearSight) continue;

      const dateRaw = row['sales date'] || '';
      if (!dateRaw) continue;
      let dateStr;
      try {
        const d = new Date(dateRaw);
        if (isNaN(d.getTime())) continue;
        dateStr = d.toISOString().split('T')[0];
      } catch(e) { continue; }

      const orderDt = new Date(dateStr + 'T00:00:00Z');
      if (orderDt < fromDt || orderDt > toDt) continue;

      const posPageName    = (row['facebook page'] || '').trim();
      const pageKey        = normCxName(posPageName);
      const pageId         = POS_PAGE_ID_MAP[pageKey] || null;
      const pageShortName  = pageId ? (PANCAKE_PAGE_META[pageId]?.short || posPageName) : posPageName;
      if (pageId) pageIdsNeeded.add(pageId);

      const amount        = parseFloat((row['unit price'] || '0').replace(/,/g, '')) || 0;
      const status        = (row['status'] || '').trim();
      if (status.toUpperCase().includes('CANCEL')) continue;

      const cxName        = (row['customer'] || '').trim();
      const contactNumber = (row['contact number'] || '').trim();

      orders.push({ date: dateStr, fsa: sellerRaw, cxName, contactNumber, posPageName, pageId, pageShortName,
        amount, status, remarks: [], upsells: 'W/O UPSELL', typeOfInq: 'FUI', remarksForOrder: '' });
    }

    // Step 2-4 — Fetch Pancake conversations + tag defs for ALL CS pages (for accurate SDI count)
    // plus any additional pages found in orders
    const pageIdList = [...new Set([...CS_ALL_PAGE_IDS, ...pageIdsNeeded])];
    const [convResults, tagResults] = await Promise.all([
      Promise.all(pageIdList.map(async pid => ({ pid, convs: await fetchPancakeConvs(pid, fromDate, toDate) }))),
      Promise.all(pageIdList.map(async pid => ({ pid, tagDefs: await fetchPancakeTagDefs(pid) }))),
    ]);

    // Build flat phone lookup: "pageId|normPhone" → conv  +  tag def map
    const convByPhone   = {}; // "pageId|normPhone" → conv
    const tagDefsMap    = {}; // pid → { tagId → tagName }
    const pageInquiries = {}; // pid → { shortName }

    for (const { pid, tagDefs } of tagResults) tagDefsMap[pid] = tagDefs;

    for (const { pid, convs } of convResults) {
      pageInquiries[pid] = { shortName: PANCAKE_PAGE_META[pid]?.short || pid };
      for (const conv of convs) {
        const phone = normPhone(getConvPhone(conv));
        const key   = pid + '|' + phone;
        if (phone && !convByPhone[key]) convByPhone[key] = conv;
      }
    }

    // Step 5 — SDI/FUI classification + tag enrichment
    // SDI = phone match found AND conv was created on toDate (PHT)
    // FUI = phone match found but conv created on a different day
    //       OR no phone match (hidden phone, name-only, or no match at all)
    // NO PAGE = order has no mapped Pancake page (excluded from SDI/CVR)
    const REMARK_TAGS = ['CALLED', 'UNATTENDED', 'CBR', 'CVC'];
    for (const order of orders) {
      if (!order.pageId) {
        order.typeOfInq = 'NO PAGE';
        continue;
      }

      const orderPhone = normPhone(order.contactNumber);
      const convKey    = order.pageId + '|' + orderPhone;
      const conv       = orderPhone ? convByPhone[convKey] : null;
      const tagDefs    = tagDefsMap[order.pageId] || {};

      if (conv && conv.has_phone !== false) {
        // Phone matched — determine SDI vs FUI by creation date in PHT vs report end date
        const convDate = toPHTDateStr(conv.inserted_at || conv.created_at);
        order.typeOfInq = (convDate === toDate) ? 'SDI' : 'FUI';

        const convTagNames = (conv.tags || []).map(id => tagDefs[id] || '').filter(Boolean);
        order.remarks = convTagNames.filter(t => REMARK_TAGS.includes(t));
        if (convTagNames.includes('AI UPSELL'))       order.upsells = 'AI UPSELL';
        else if (convTagNames.includes('FSA UPSELL')) order.upsells = 'FSA UPSELL';
        if (convTagNames.includes('FSA SPIELS'))           order.remarksForOrder = 'FSA SPIELS';
        else if (convTagNames.includes('TELECONSULT'))     order.remarksForOrder = 'TELECONSULT';
        else if (convTagNames.includes('AUTO ORDER'))      order.remarksForOrder = 'AUTO ORDER';
      }
      // else: hidden phone, no phone in CSV, or no match → FUI (already default)
    }

    // Step 6 — Per-FSA inquiry count via pages.fm v2 API (both CS pages combined)
    // PHT = UTC+8; convert date range to Unix timestamps
    const sinceTs = Math.floor(new Date(fromDate + 'T00:00:00+08:00').getTime() / 1000);
    const untilTs = Math.floor(new Date(toDate   + 'T23:59:59+08:00').getTime() / 1000);

    const v2ConvArrays = await Promise.all(
      CS_INQUIRY_PAGES.map(({ pageId, token }) => fetchPancakeConvsV2(pageId, token, sinceTs, untilTs))
    );
    // Count conversations per Pancake assignee name (all pages combined)
    // Count all conversations assigned to each FSA that were active on the date range.
    // The v2 API since/until already filters by last-activity date, so every conv
    // returned was handled by someone on the target day.
    const fsaInquiries = {}; // pancakeName → count
    for (const convs of v2ConvArrays) {
      for (const conv of convs) {
        for (const user of (conv.current_assign_users || [])) {
          const name = (user.name || '').trim();
          if (name) fsaInquiries[name] = (fsaInquiries[name] || 0) + 1;
        }
      }
    }

    res.json({ from: fromDate, to: toDate, orders, pageInquiries, fsaInquiries, fsaPriority: CS_FSA_PRIORITY });
  } catch(e) {
    console.error('aov-cvr error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`🐻 NDAP Dashboard at http://localhost:${PORT}`));
