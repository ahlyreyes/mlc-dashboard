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
const META_TOKEN_1 = process.env.META_TOKEN_1 || META_ACCESS_TOKEN;
const META_TOKEN_2 = process.env.META_TOKEN_2 || META_ACCESS_TOKEN;

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
];

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
    // Add locked column if it doesn't exist (migration)
    await pool.query(`ALTER TABLE page_budgets ADD COLUMN IF NOT EXISTS locked BOOLEAN DEFAULT FALSE`);
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
  const user = AUTHORIZED_USERS.find(u => u.email === email);
  if (!user) return done(null, false, { message: 'unauthorized' });
  return done(null, { ...user, googleId: profile.id, photo: profile.photos?.[0]?.value });
}));

passport.serializeUser((user, done) => done(null, user.email));
passport.deserializeUser((email, done) => {
  const user = AUTHORIZED_USERS.find(u => u.email === email);
  done(null, user || false);
});

const requireAuth = (req, res, next) => {
  if (req.isAuthenticated()) return next();
  res.redirect('/login');
};
const PANCAKE_CSV_URLS = [
  process.env.PANCAKE_CSV_URL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRRmBJlKTC1iFdU5mcZ8sQlWkHuxAtYxezNnAO1ggj1wKh1_ki045CTbDw6aV2FvVL5tBV42gMHilio/pub?gid=0&single=true&output=csv',
  process.env.PANCAKE_CSV_URL_APRIL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSWfevqFhSyLoIFwvwFFdgFY3NzyhTOu6nbW3_2CfhI460Etz60TPWH2yA1TkVfG2y439O43BOvXHb4/pub?gid=0&single=true&output=csv'
];

// MLC mainfile — source for AOV & CVR FSA report (John Hovey Cabatic, Lex Dela Cruz)
const MLC_MAINFILE_CSV_URL = process.env.MLC_MAINFILE_CSV_URL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRTziENAURT5p8ix0v0FOizV9a_i-p4Igeovw21jv09aqbJbqvsjKMEftGVfG8Dm0rmVvcKUv0MQkul/pub?output=csv';

const PANCAKE_TOKEN = process.env.PANCAKE_TOKEN || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbmZvIjp7Im9zIjoxLCJjbGllbnRfaXAiOiI2NC4yMjQuOTcuMTU0IiwiYnJvd3NlciI6MSwiZGV2aWNlX3R5cGUiOjN9LCJuYW1lIjoiQ2xhcmljZSBEYWxvbmRvbmFuIiwiZXhwIjoxNzgyMzA1MTQzLCJhcHBsaWNhdGlvbiI6MSwidWlkIjoiMWNjYjM3YTgtZjQ4NS00ZjdiLWJiMWQtZjhjNTQ0Nzg4NWM2Iiwic2Vzc2lvbl9pZCI6ImQ3MDM5OWFhLTIxYTMtNDRmZi05ZmI5LWVmMDBjNzI3YmE2YSIsImlhdCI6MTc3NDUyOTE0MywiZmJfaWQiOiIxMjIxMTI2MzIwNDg5OTY4NTUiLCJsb2dpbl9zZXNzaW9uIjpudWxsLCJmYl9uYW1lIjoiQ2xhcmljZSBEYWxvbmRvbmFuIn0.FUzLeVPKVMDqbruljozSc93SBsX76gj0HMfeiv4kpAA';

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
      `&limit=500&page=${page}&access_token=${PANCAKE_TOKEN}`;
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
    const res = await fetchJson(`https://pancake.biz/api/v1/pages/${pageId}/settings?access_token=${PANCAKE_TOKEN}`);
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
  // --- CLEAR SIGHT --- Token 1
  { id: 'act_553848412391460',  name: 'Iniwan Lang Pala', currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_1 },
  { id: 'act_2825452284312899', name: 'Ad Account 2',     currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_1 },
  { id: 'act_827911349880726',  name: 'Cheska Del Mundo', currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_1 },
  // --- CLEAR SIGHT --- Token 2
  { id: 'act_1264536714635179', name: 'Nhur Lita',        currency: 'PHP', product: 'CLEAR SIGHT', token: META_TOKEN_2 },
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
      const isClearSight = rowText.includes('clear sight');
      const isHaplunas   = rowText.includes('haplunas');

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
    }

    console.log(`📊 Pancake parse: ${countedCS} Clear Sight rows counted, ${skipCancel} cancelled, ${skipNoDate} no-date, ${skipNonClearSight} non-CS`);
    console.log('📅 clearSightByDate totals:', Object.entries(clearSightByDate).map(([d,v])=>`${d}:₱${v.sales.toFixed(0)}(${v.orders})`).join(', '));

    const result = { salesByDate, clearSightByDate };
    cacheSet(pancakeCache, 'pancake', result);
    return result;
  } catch(e) {
    console.error('Pancake CSV error:', e.message);
    return { salesByDate: {}, clearSightByDate: {} };
  }
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
app.get('/roas-report',       requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'roas-report.html')));
app.get('/income-statement',  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'income-statement.html')));
app.get('/aov-cvr-report',    requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'aov-cvr-report.html')));
app.get('/sales-report',      requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'sales-report.html')));
app.get('/',                  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/ndap', requireAuth, async (req, res) => {
  try {
    const endDate = req.query.endDate || new Date().toISOString().split('T')[0];
    const days = parseInt(req.query.days || 3);

    const dates = [];
    for (let i = days - 1; i >= 0; i--) {
      const parts = endDate.split('-').map(Number);
      const d = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2]));
      d.setUTCDate(d.getUTCDate() - i);
      dates.push(d.toISOString().split('T')[0]);
    }

    const [pancakeData, budgetMap, activeAdsList] = await Promise.all([
      fetchPancakeSalesByDate(), fetchAllBudgets(), fetchAllActiveAds()
    ]);
    const { salesByDate, clearSightByDate } = pancakeData;

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

    const result = { dates, campaigns: activeCampaigns, clearSightByDate };
    res.json(result);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/aov-cvr', requireAuth, async (req, res) => {
  try {
    const fromDate = req.query.from || new Date().toISOString().split('T')[0];
    const toDate   = req.query.to   || fromDate;

    // Step 1 — Fetch POS orders from MLC mainfile
    const rows = parseCSV(await fetchRaw(MLC_MAINFILE_CSV_URL));

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

      const amount  = parseFloat((row['unit price'] || '0').replace(/,/g, '')) || 0;
      const status  = (row['status'] || '').trim();
      const cxName  = (row['customer'] || '').trim();

      orders.push({ date: dateStr, fsa: sellerRaw, cxName, posPageName, pageId, pageShortName,
        amount, status, remarks: [], upsells: 'W/O UPSELL', typeOfInq: 'SDI', remarksForOrder: '' });
    }

    // Step 2-4 — Fetch Pancake conversations + tag defs for ALL CS pages (for accurate SDI count)
    // plus any additional pages found in orders
    const pageIdList = [...new Set([...CS_ALL_PAGE_IDS, ...pageIdsNeeded])];
    const [convResults, tagResults] = await Promise.all([
      Promise.all(pageIdList.map(async pid => ({ pid, convs: await fetchPancakeConvs(pid, fromDate, toDate) }))),
      Promise.all(pageIdList.map(async pid => ({ pid, tagDefs: await fetchPancakeTagDefs(pid) }))),
    ]);

    const convLookup   = {}; // pid → { normName → { conv, isInRange, tagDefs } }
    const tagDefsMap   = {}; // pid → { tagId → tagName }
    const pageInquiries = {}; // pid → { shortName, sdi }

    for (const { pid, tagDefs } of tagResults) tagDefsMap[pid] = tagDefs;

    for (const { pid, convs } of convResults) {
      convLookup[pid] = {};
      const tagDefs = tagDefsMap[pid] || {};
      let sdiCount = 0;
      for (const conv of convs) {
        const convDate = (conv.inserted_at || '').split('T')[0];
        const isInRange = convDate >= fromDate && convDate <= toDate;
        if (isInRange) sdiCount++;
        const cxName = (conv.from && conv.from.name) || (conv.customers && conv.customers[0] && conv.customers[0].name) || '';
        const key = normCxName(cxName);
        if (key && !convLookup[pid][key]) {
          convLookup[pid][key] = { conv, isInRange, tagDefs };
        }
      }
      pageInquiries[pid] = { shortName: PANCAKE_PAGE_META[pid]?.short || pid, sdi: sdiCount };
    }

    // Step 5 — Enrich orders with Pancake tag data
    const REMARK_TAGS     = ['CALLED', 'UNATTENDED', 'CBR', 'CVC'];
    for (const order of orders) {
      if (!order.pageId) continue;
      const match = (convLookup[order.pageId] || {})[normCxName(order.cxName)];
      if (!match) continue;
      const { conv, isInRange, tagDefs } = match;
      order.typeOfInq = isInRange ? 'SDI' : 'FUI';
      const convTagNames = (conv.tags || []).map(id => tagDefs[id] || '').filter(Boolean);
      order.remarks = convTagNames.filter(t => REMARK_TAGS.includes(t));
      if (convTagNames.includes('AI UPSELL'))       order.upsells = 'AI UPSELL';
      else if (convTagNames.includes('FSA UPSELL')) order.upsells = 'FSA UPSELL';
      if (convTagNames.includes('FSA SPIELS'))           order.remarksForOrder = 'FSA SPIELS';
      else if (convTagNames.includes('TELECONSULT'))     order.remarksForOrder = 'TELECONSULT';
      else if (convTagNames.includes('AUTO ORDER'))      order.remarksForOrder = 'AUTO ORDER';
    }

    res.json({ from: fromDate, to: toDate, orders, pageInquiries, fsaPriority: CS_FSA_PRIORITY });
  } catch(e) {
    console.error('aov-cvr error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`🐻 NDAP Dashboard at http://localhost:${PORT}`));
