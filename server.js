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
  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim());
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
  try {
    const allCsvs = await Promise.all(PANCAKE_CSV_URLS.map(url => fetchRaw(url)));
    const rows = allCsvs.flatMap(csv => parseCSV(csv));
    // Exclude CRD department sellers — their sales should not count in FSD ROAS
    const EXCLUDED_SELLERS = ['Marj Adana', 'Shan Chai Bertes'];

    const salesByDate = {};
    for (const row of rows) {
      const seller = row['Assigning seller'] || '';
      if (!seller) continue;
      if (EXCLUDED_SELLERS.some(s => seller.trim().toLowerCase() === s.toLowerCase())) continue;
      const adId = row['Ads'] || '';
      if (!adId) continue;
      const price = parseFloat((row['Unit price'] || '0').replace(/,/g, '')) || 0;
      const dateRaw = row['Sales Date'] || '';
      if (!dateRaw) continue;
      let dateStr;
      try {
        const d = new Date(dateRaw);
        if (isNaN(d.getTime())) continue;
        dateStr = formatDate(d);
      } catch(e) { continue; }
      const status = (row['Status'] || '').trim().toUpperCase();
      if (!salesByDate[dateStr]) salesByDate[dateStr] = {};
      if (!salesByDate[dateStr][adId]) salesByDate[dateStr][adId] = {
        sales: 0, orders: 0,
        delivered: 0, deliveredValue: 0,
        rts: 0, rtsValue: 0,
        shipped: 0, shippedValue: 0
      };
      salesByDate[dateStr][adId].sales += price;
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
    }
    return salesByDate;
  } catch(e) {
    console.error('Pancake CSV error:', e.message);
    return {};
  }
}

async function fetchAccountInsights(account, date) {
  const token = account.token || '';
  const fxRate = FX_TO_PHP[account.currency || 'PHP'] || 1;

  // Fetch all pages of ad-level insights
  const allRows = [];
  let nextUrl = `https://graph.facebook.com/v19.0/${account.id}/insights` +
    `?fields=ad_id,ad_name,campaign_id,campaign_name,spend,impressions,clicks,cost_per_action_type,actions` +
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
        return [{
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
      }
      return [];
    }

    return allRows.map(r => {
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
        costPerMessage,
        messagesStarted
      };
    });
  } catch(e) {
    console.error(`❌ fetchAccountInsights failed for ${account.name} (${account.id}): ${e.message}`);
    return [];
  }
}

async function fetchBudgetsForAccount(account) {
  const fxRate = FX_TO_PHP[account.currency || 'PHP'] || 1;
  const result = {};
  try {
    let nextUrl = `https://graph.facebook.com/v19.0/${account.id}/ads` +
      `?fields=id,adset{daily_budget,campaign{daily_budget}}&limit=500&access_token=${account.token || ''}`;
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
  return result;
}

async function fetchAllBudgets() {
  const maps = await Promise.all(AD_ACCOUNTS.map(acc => fetchBudgetsForAccount(acc)));
  return Object.assign({}, ...maps);
}

// ── IN-MEMORY CACHE ──
const ndapCache = new Map();
const CACHE_TTL_MS = 15 * 60 * 1000; // 15 minutes

function getCacheKey(endDate, days) { return `${endDate}__${days}`; }

function getCached(key) {
  const entry = ndapCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) { ndapCache.delete(key); return null; }
  return entry.data;
}

function setCache(key, data) {
  ndapCache.set(key, { data, ts: Date.now() });
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
app.get('/roas-report',       requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'roas-report.html')));
app.get('/income-statement',  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'income-statement.html')));
app.get('/aov-cvr-report',    requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'aov-cvr-report.html')));
app.get('/sales-report',      requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'sales-report.html')));
app.get('/',                  requireAuth, (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/ndap', requireAuth, async (req, res) => {
  try {
    const endDate = req.query.endDate || new Date().toISOString().split('T')[0];
    const days = parseInt(req.query.days || 3);

    const cacheKey = getCacheKey(endDate, days);
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`✅ Cache hit: ${cacheKey}`);
      return res.json(cached);
    }

    const dates = [];
    for (let i = days - 1; i >= 0; i--) {
      const parts = endDate.split('-').map(Number);
      const d = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2]));
      d.setUTCDate(d.getUTCDate() - i);
      dates.push(d.toISOString().split('T')[0]);
    }

    const [salesByDate, budgetMap] = await Promise.all([
      fetchPancakeSalesByDate(), fetchAllBudgets()
    ]);

    const insightsByDate = {};
    await Promise.all(dates.map(async (date) => {
      const all = await Promise.all(AD_ACCOUNTS.map(acc => fetchAccountInsights(acc, date)));
      insightsByDate[date] = all.flat();
    }));

    const campaignMap = {};
    for (const date of dates) {
      for (const row of (insightsByDate[date] || [])) {
        const key = `${row.accountName}|||${row.campaignId}`;
        if (!campaignMap[key]) {
          campaignMap[key] = {
            campaignId: row.campaignId, campaignName: row.campaignName,
            adId: row.adId, adName: row.adName,
            accountName: row.accountName,
            product: row.product || '',
            budget: budgetMap[row.adId] || 0,
            dates: {}
          };
        }
        const sales = salesByDate[date]?.[row.adId] || { sales: 0, orders: 0, delivered: 0, deliveredValue: 0, rts: 0, rtsValue: 0, shipped: 0, shippedValue: 0 };

        // Accumulate per-ad data into campaign totals (fixes multi-ad campaigns)
        if (!campaignMap[key].dates[date]) {
          campaignMap[key].dates[date] = {
            spend: 0, grossSales: 0, orders: 0,
            impressions: 0, clicks: 0,
            costPerMessage: 0, messagesStarted: 0,
            delivered: 0, deliveredValue: 0,
            rts: 0, rtsValue: 0, shipped: 0, shippedValue: 0
          };
        }
        const d = campaignMap[key].dates[date];
        d.spend        += row.spend;
        d.grossSales   += sales.sales || 0;
        d.orders       += sales.orders || 0;
        d.impressions  += row.impressions;
        d.clicks       += row.clicks;
        d.messagesStarted += row.messagesStarted || 0;
        d.costPerMessage  += row.costPerMessage || 0;
        d.delivered    += sales.delivered || 0;
        d.deliveredValue += sales.deliveredValue || 0;
        d.rts          += sales.rts || 0;
        d.rtsValue     += sales.rtsValue || 0;
        d.shipped      += sales.shipped || 0;
        d.shippedValue += sales.shippedValue || 0;
      }
    }

    // Calculate derived metrics after accumulation
    for (const campaign of Object.values(campaignMap)) {
      for (const d of Object.values(campaign.dates)) {
        d.roas    = d.spend > 0 ? d.grossSales / d.spend : 0;
        d.cpp     = d.orders > 0 ? d.spend / d.orders : 0;
        d.cpm     = d.messagesStarted > 0 ? d.spend / d.messagesStarted : 0;
        d.delRate = (d.delivered + d.rts) > 0 ? (d.delivered / (d.delivered + d.rts) * 100) : null;
      }
    }

    const campaigns = Object.values(campaignMap).map(c => {
      let ts = 0, tsp = 0, to = 0, tDel = 0, tRts = 0, tShip = 0, tDelVal = 0, tRtsVal = 0;
      for (const d of Object.values(c.dates)) {
        ts += d.grossSales; tsp += d.spend; to += d.orders;
        tDel += d.delivered || 0; tRts += d.rts || 0; tShip += d.shipped || 0;
        tDelVal += d.deliveredValue || 0; tRtsVal += d.rtsValue || 0;
      }
      const overallDelRate = (tDel + tRts) > 0 ? (tDel / (tDel + tRts) * 100) : null;
      return { ...c, totalSales: ts, totalSpend: tsp, totalOrders: to,
        totalRoas: tsp > 0 ? ts / tsp : 0,
        totalDelivered: tDel, totalRts: tRts, totalShipped: tShip,
        totalDeliveredValue: tDelVal, totalRtsValue: tRtsVal, overallDelRate };
    });

    // Filter: only show campaigns that had spend in at least one day of the selected date range
    const activeCampaigns = campaigns.filter(c => c.totalSpend > 0);

    activeCampaigns.sort((a, b) => {
      if (a.accountName !== b.accountName) return a.accountName.localeCompare(b.accountName);
      return a.campaignName.localeCompare(b.campaignName);
    });

    const result = { dates, campaigns: activeCampaigns };
    setCache(cacheKey, result);
    res.json(result);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`🐻 NDAP Dashboard at http://localhost:${PORT}`));
