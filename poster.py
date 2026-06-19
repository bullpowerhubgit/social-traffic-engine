#!/usr/bin/env python3
"""
Social Traffic Engine — MAXIMUM TUNING v2.0
Real Reddit + LinkedIn posting, IndexNow, Google Trends RSS, SQLite state,
content repurposing machine, turbo scheduler.
"""
import asyncio
import base64
import json
import logging
import os
import random
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import quote

import aiohttp
import aiosqlite
import anthropic
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("social-traffic-turbo")

PORT          = int(os.getenv("PORT", "8080"))
APP_URL       = os.getenv("APP_URL", "https://social-traffic-engine-production.up.railway.app")
DB_PATH       = os.getenv("DB_PATH", "/tmp/social_engine.db")

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

MC_API_KEY    = os.getenv("MAILCHIMP_API_KEY", "")
MC_SERVER     = os.getenv("MAILCHIMP_SERVER_PREFIX", "us7")
MC_LIST_ID    = os.getenv("MAILCHIMP_LIST_ID", "")
KV_API_KEY    = os.getenv("KLAVIYO_API_KEY", "")

REDDIT_CLIENT = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER   = os.getenv("REDDIT_USERNAME", "")
REDDIT_PASS   = os.getenv("REDDIT_PASSWORD", "")

LINKEDIN_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_URN   = os.getenv("LINKEDIN_PERSON_URN", "")

INDEXNOW_KEY  = os.getenv("INDEXNOW_KEY",
    str(uuid.uuid5(uuid.NAMESPACE_URL, APP_URL)).replace("-", ""))

PRODUCTS = [
    {"name": "SteuercockPit",              "url": "https://bullpower-steuercockpit.netlify.app",                         "pitch": "KI-Buchhaltung für Selbstständige — Abo-Verwaltung, Bank-CSV-Analyse, ELSTER-Vorbereitung"},
    {"name": "BullPower Hub Bundle",       "url": "https://bullpower-hub-portal.netlify.app",                            "pitch": "8 KI-Automatisierungs-Tools für Shopify + E-Commerce — 14 Tage kostenlos"},
    {"name": "Shopify Acquisition Engine", "url": "https://shopify-acquisition-engine-production.up.railway.app",        "pitch": "KI findet profitable Shopify-Produkte automatisch — Trending items, Preisoptimierung"},
    {"name": "Lead Capture",               "url": "https://bullpower-lead.netlify.app",                                  "pitch": "KI analysiert deinen Shopify-Shop kostenlos — SEO, Conversion, Preise"},
    {"name": "iComeAuto",                  "url": "https://bullpower-icomeauto.netlify.app",                             "pitch": "Income Automation Platform — passives Einkommen vollautomatisch"},
    {"name": "SEO Turbo Tools",            "url": "https://seo-turbo-tools-production.up.railway.app",                   "pitch": "KI-gestützte SEO-Analyse, Keyword Research & Meta-Generator"},
    {"name": "Analytics Marketing Pro",    "url": "https://analytics-marketing-pro-production.up.railway.app",           "pitch": "Klaviyo, Mailchimp & Facebook Pixel vollautomatisch verbunden"},
    {"name": "CreatorAI Ultra",            "url": "https://creatorai-ultra-production.up.railway.app",                   "pitch": "KI Content Creator — Blog, Video-Skripte, Social Media Posts vollautomatisch"},
    {"name": "Cognitive Symphony",         "url": "https://cognitive-symphony-production.up.railway.app",                "pitch": "KI Business Analyse & Automatisierung für E-Commerce Unternehmer"},
    {"name": "Shopify Automaton Suite",    "url": "https://shopify-automaton-suite-production-e405.up.railway.app",      "pitch": "Vollautomatische Shopify Suite mit Amazon & AliExpress Integration"},
]

SITEMAPS = [
    "https://shopify-automaton-suite-production-e405.up.railway.app/sitemap.xml",
    "https://seo-turbo-tools-production.up.railway.app/sitemap.xml",
    "https://analytics-marketing-pro-production.up.railway.app/sitemap.xml",
    "https://shopify-acquisition-engine-production.up.railway.app/sitemap.xml",
    "https://bullpower-steuercockpit.netlify.app/sitemap.xml",
    "https://bullpower-hub-portal.netlify.app/sitemap.xml",
    "https://bullpower-lead.netlify.app/sitemap.xml",
    "https://seo-traffic-engine-production.up.railway.app/sitemap.xml",
]

REDDIT_SUBS = ["entrepreneur", "ecommerce", "SEO", "shopify", "digital_marketing",
                "passive_income", "smallbusiness", "marketing"]

# ── In-memory stats ────────────────────────────────────────────────────────────
_stats: dict = {
    "posts_generated": 0,
    "last_run": None,
    "errors": 0,
    "last_trends_fetch": None,
    "posts_by_platform": {},
}


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                content TEXT,
                url TEXT,
                posted_at TEXT NOT NULL,
                success INTEGER DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS keyword_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE NOT NULL,
                priority INTEGER DEFAULT 5,
                source TEXT DEFAULT 'manual',
                used_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS scheduler_state (
                task TEXT PRIMARY KEY,
                last_run INTEGER DEFAULT 0
            )
        """)
        await db.commit()


async def log_post(platform: str, content: str, url: str, success: bool = True):
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO posts_sent (platform, content, url, posted_at, success) VALUES (?, ?, ?, ?, ?)",
            (platform, content[:500], url, now, int(success))
        )
        await db.commit()
    _stats["posts_generated"] += 1
    _stats["posts_by_platform"][platform] = _stats["posts_by_platform"].get(platform, 0) + 1


async def task_due(task: str, interval: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT last_run FROM scheduler_state WHERE task = ?", (task,))
        row = await cursor.fetchone()
        last_run = row[0] if row else 0
        if int(time.time()) - last_run >= interval:
            await db.execute(
                "INSERT OR REPLACE INTO scheduler_state (task, last_run) VALUES (?, ?)",
                (task, int(time.time()))
            )
            await db.commit()
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# CORE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def send_telegram(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text[:4096],
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=12),
            )
    except Exception as e:
        log.warning(f"Telegram: {e}")


async def call_claude(prompt: str, max_tokens: int = 600) -> str:
    if not ANTHROPIC_KEY:
        return "[ANTHROPIC_KEY missing]"
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.error(f"Claude error: {e}")
        return ""


async def klaviyo_track(event: str, props: dict):
    if not KV_API_KEY:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                "https://a.klaviyo.com/api/events/",
                headers={"Authorization": f"Klaviyo-API-Key {KV_API_KEY}",
                         "revision": "2024-06-15", "Content-Type": "application/json"},
                json={"data": {"type": "event", "attributes": {
                    "metric": {"data": {"type": "metric", "attributes": {"name": event}}},
                    "properties": props,
                    "time": datetime.now(timezone.utc).isoformat(),
                }}},
                timeout=aiohttp.ClientTimeout(total=8),
            )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE TRENDS RSS — FREE TRENDING KEYWORDS
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_google_trends(geo: str = "DE") -> list[str]:
    url = f"https://trends.google.com/trends/trendingsearches/daily/rss?geo={geo}"
    keywords: list[str] = []
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15),
                             headers={"User-Agent": "Mozilla/5.0 (compatible; SocialBot/2.0)"}) as r:
                if r.status == 200:
                    text = await r.text()
                    root = ET.fromstring(text)
                    for item in root.findall(".//item"):
                        el = item.find("title")
                        if el is not None and el.text:
                            kw = el.text.strip().lower()
                            if any(t in kw for t in ["shop", "amazon", "ebay", "ki", "ai",
                                                      "geld", "online", "digital", "seo",
                                                      "app", "tool", "auto", "business"]):
                                keywords.append(kw)
    except Exception as e:
        log.warning(f"Google Trends: {e}")
    return keywords[:20]


async def task_refresh_trends():
    trending = await fetch_google_trends("DE")
    if not trending:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        for kw in trending:
            await db.execute(
                "INSERT OR IGNORE INTO keyword_queue (keyword, priority, source) VALUES (?, 9, 'google_trends')",
                (kw,)
            )
        await db.commit()
    _stats["last_trends_fetch"] = datetime.now(timezone.utc).isoformat()
    log.info(f"Trends: added {len(trending)} keywords")
    await send_telegram(f"📈 <b>Google Trends:</b> {len(trending)} neue Keywords\n" +
                        ", ".join(trending[:5]))


# ═══════════════════════════════════════════════════════════════════════════════
# INDEXNOW — INSTANT BING / YANDEX / SEZNAM INDEXING
# ═══════════════════════════════════════════════════════════════════════════════

async def indexnow_ping(urls: list[str]) -> bool:
    if not urls:
        return False
    host = APP_URL.replace("https://", "").replace("http://", "")
    payload = {
        "host": host,
        "key": INDEXNOW_KEY,
        "keyLocation": f"{APP_URL}/{INDEXNOW_KEY}.txt",
        "urlList": urls[:100],
    }
    endpoints = [
        "https://api.indexnow.org/indexnow",
        "https://www.bing.com/indexnow",
        "https://yandex.com/indexnow",
    ]
    ok = False
    async with aiohttp.ClientSession() as s:
        for ep in endpoints:
            try:
                async with s.post(ep, json=payload,
                                  headers={"Content-Type": "application/json; charset=utf-8"},
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status in (200, 202):
                        ok = True
            except Exception as e:
                log.warning(f"IndexNow {ep}: {e}")
    return ok


async def ping_sitemaps():
    pinged = 0
    async with aiohttp.ClientSession() as s:
        for sm in SITEMAPS:
            for ep in [f"https://www.google.com/ping?sitemap={quote(sm, safe='')}",
                       f"https://www.bing.com/ping?sitemap={quote(sm, safe='')}"]:
                try:
                    await s.get(ep, timeout=aiohttp.ClientTimeout(total=8))
                    pinged += 1
                except Exception:
                    pass
    # IndexNow for all sitemap URLs
    await indexnow_ping(SITEMAPS)
    log.info(f"Sitemap ping: {pinged}/{len(SITEMAPS)*2}")
    return pinged


# ═══════════════════════════════════════════════════════════════════════════════
# REAL REDDIT POSTING
# ═══════════════════════════════════════════════════════════════════════════════

_reddit_token: str = ""
_reddit_token_expiry: float = 0.0


async def reddit_get_token() -> str:
    global _reddit_token, _reddit_token_expiry
    if _reddit_token and time.time() < _reddit_token_expiry - 60:
        return _reddit_token
    if not all([REDDIT_CLIENT, REDDIT_SECRET, REDDIT_USER, REDDIT_PASS]):
        return ""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=aiohttp.BasicAuth(REDDIT_CLIENT, REDDIT_SECRET),
                data={"grant_type": "password", "username": REDDIT_USER, "password": REDDIT_PASS},
                headers={"User-Agent": "SocialTrafficBot/2.0"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                data = await r.json()
                _reddit_token = data.get("access_token", "")
                _reddit_token_expiry = time.time() + data.get("expires_in", 3600)
                return _reddit_token
    except Exception as e:
        log.error(f"Reddit auth: {e}")
        return ""


async def post_to_reddit(title: str, body: str, subreddit: str) -> bool:
    token = await reddit_get_token()
    if not token:
        return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://oauth.reddit.com/api/submit",
                headers={"Authorization": f"bearer {token}",
                         "User-Agent": "SocialTrafficBot/2.0"},
                data={"sr": subreddit, "kind": "self",
                      "title": title[:300], "text": body[:10000], "nsfw": "false"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                resp = await r.json()
                if r.status in (200, 201) and not resp.get("json", {}).get("errors"):
                    log.info(f"Reddit r/{subreddit}: posted")
                    await log_post("reddit", f"r/{subreddit}: {title}", "", True)
                    return True
                log.warning(f"Reddit r/{subreddit}: {resp}")
    except Exception as e:
        log.error(f"Reddit post: {e}")
    await log_post("reddit", title, "", False)
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# REAL LINKEDIN POSTING
# ═══════════════════════════════════════════════════════════════════════════════

async def post_to_linkedin(text: str) -> bool:
    if not LINKEDIN_TOKEN or not LINKEDIN_URN:
        return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://api.linkedin.com/v2/ugcPosts",
                headers={"Authorization": f"Bearer {LINKEDIN_TOKEN}",
                         "Content-Type": "application/json",
                         "X-Restli-Protocol-Version": "2.0.0"},
                json={
                    "author": f"urn:li:person:{LINKEDIN_URN}",
                    "lifecycleState": "PUBLISHED",
                    "specificContent": {
                        "com.linkedin.ugc.ShareContent": {
                            "shareCommentary": {"text": text[:3000]},
                            "shareMediaCategory": "NONE",
                        }
                    },
                    "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status in (200, 201):
                    log.info("LinkedIn: posted")
                    await log_post("linkedin", text, "", True)
                    return True
                log.error(f"LinkedIn {r.status}: {await r.text()}")
    except Exception as e:
        log.error(f"LinkedIn: {e}")
    await log_post("linkedin", text, "", False)
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# MAILCHIMP CAMPAIGN
# ═══════════════════════════════════════════════════════════════════════════════

async def send_mailchimp_campaign(subject: str, html: str) -> bool:
    if not all([MC_API_KEY, MC_LIST_ID]):
        return False
    auth = "Basic " + base64.b64encode(f"any:{MC_API_KEY}".encode()).decode()
    base = f"https://{MC_SERVER}.api.mailchimp.com/3.0"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{base}/campaigns",
                              headers={"Authorization": auth},
                              json={"type": "regular",
                                    "recipients": {"list_id": MC_LIST_ID},
                                    "settings": {"subject_line": subject,
                                                 "from_name": "BullPower Hub",
                                                 "reply_to": "info@bullpower-hub.de"}},
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return False
                cid = (await r.json()).get("id", "")

            if not cid:
                return False

            async with s.put(f"{base}/campaigns/{cid}/content",
                             headers={"Authorization": auth},
                             json={"html": html},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    return False

            async with s.post(f"{base}/campaigns/{cid}/actions/send",
                              headers={"Authorization": auth},
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 204:
                    log.info(f"Mailchimp campaign {cid} sent")
                    return True
    except Exception as e:
        log.error(f"Mailchimp: {e}")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# CONTENT GENERATORS
# ═══════════════════════════════════════════════════════════════════════════════

async def gen_reddit(product: dict) -> tuple[str, str]:
    sub = random.choice(REDDIT_SUBS)
    raw = await call_claude(
        f"Write a helpful Reddit post for r/{sub} in English. "
        f"Product: {product['name']} — {product['pitch']}. URL: {product['url']}\n"
        "Format exactly:\nTITLE: <title max 120 chars>\nBODY: <3-4 authentic helpful sentences, link at end>\n"
        "No marketing speak. Write as a real user sharing something useful.",
        max_tokens=400
    )
    title, body = product["name"], f"{product['pitch']}\n\n{product['url']}"
    for line in raw.split("\n"):
        if line.startswith("TITLE:"):
            title = line.split(":", 1)[1].strip()
        elif line.startswith("BODY:"):
            body = line.split(":", 1)[1].strip()
    return title, body


async def gen_linkedin(product: dict, topic: str = "") -> str:
    return await call_claude(
        f"Schreibe einen professionellen LinkedIn-Post auf Deutsch.\n"
        f"Produkt: {product['name']} — {product['pitch']}\nURL: {product['url']}"
        + (f"\nThema: {topic}" if topic else "") +
        "\n3-5 Zeilen, Business-Kontext, 2-3 Hashtags (#Shopify #Ecommerce #Automatisierung), "
        "URL am Ende. Authentisch, kein übertriebenes Marketing.",
        max_tokens=350
    )


async def gen_hn(product: dict) -> str:
    return await call_claude(
        f"Write a Hacker News 'Show HN' post in English.\n"
        f"Product: {product['name']}. Technical aspect: {product['pitch']}. URL: {product['url']}\n"
        "Format: TITLE: Show HN: <title 80 chars>\nCOMMENT: <2-3 technical sentences>",
        max_tokens=300
    )


async def gen_quora(product: dict) -> str:
    questions = [
        "What are the best tools to automate a Shopify store?",
        "How can I increase organic traffic to my e-commerce store?",
        "What SEO tools are worth paying for in 2025?",
        "How do I automate my online business income?",
        "What AI tools help with e-commerce automation?",
    ]
    q = random.choice(questions)
    return await call_claude(
        f"Write a helpful Quora answer in English to: '{q}'\n"
        f"Naturally mention {product['name']} — {product['pitch']} (URL: {product['url']}) "
        "as one of several options. 3-4 sentences. General advice first, then product mention.",
        max_tokens=350
    )


async def gen_pr(product: dict) -> str:
    return await call_claude(
        f"Write a short press release in English for prlog.org/openpr.com.\n"
        f"Product: {product['name']} — {product['pitch']}. URL: {product['url']}\n"
        "Headline + 2 paragraphs. Professional, newsworthy, not overly promotional.",
        max_tokens=400
    )


async def gen_full_repurpose_pack(title: str, url: str, keyword: str,
                                   excerpt: str, product: dict | None = None) -> dict:
    """Generate all platform content in parallel for an inbound article."""
    prod = product or random.choice(PRODUCTS)

    prompts = {
        "twitter1": (
            f"Twitter thread tweet 1/3 in German. Hook tweet about: '{title}'. "
            f"Max 270 chars. Emoji, curiosity. Keyword: {keyword}. No URL in tweet 1.", 150
        ),
        "twitter2": (
            f"Twitter thread tweet 2/3 in German. Main insights from: '{title}'. "
            f"Excerpt: {excerpt[:300]}. 3 bullet points max 270 chars total.", 200
        ),
        "twitter3": (
            f"Twitter thread tweet 3/3 in German. CTA + URL: {url}\n"
            f"Relevant hashtags for {keyword}. Max 270 chars.", 150
        ),
        "linkedin": (
            f"LinkedIn post auf Deutsch für Artikel: '{title}'\nURL: {url}\nKeyword: {keyword}\n"
            f"150-200 Wörter, professionell, B2B, 3 Hashtags, Link am Ende.", 400
        ),
        "reddit_title": (
            f"Reddit post title in English for article '{title}' about {keyword}. "
            f"Max 120 chars. Curious, helpful, not spammy.", 80
        ),
        "reddit_body": (
            f"Reddit post body in English. Article: '{title}'\nURL: {url}\nExcerpt: {excerpt[:300]}\n"
            f"100-150 words. Helpful, authentic, link naturally at end.", 300
        ),
        "quora": (
            f"Quora answer in English related to '{keyword}'. Mention article '{title}' at {url} "
            "as a helpful resource. 3-4 sentences, natural mention.", 300
        ),
        "hn": (
            f"Hacker News Show HN post. Title: 'Show HN: {title[:60]}'\n"
            f"Comment: 2-3 technical sentences about what's interesting. URL: {url}", 250
        ),
        "pinterest": (
            f"Pinterest description in German for: '{title}'\nKeyword: {keyword}\n"
            "100 chars max. Keyword-rich, visual language.", 100
        ),
        "tiktok": (
            f"TikTok/YouTube Short script in German. Topic: '{title}'\nKeyword: {keyword}\n"
            "Hook (5s) + 3 points (15s each) + CTA. Total ~60 seconds. Energetic.", 400
        ),
        "email_subject": (
            f"Email subject line in German for article '{title}'. Max 60 chars. Curiosity gap.", 80
        ),
    }

    async def _gen(key: str, prompt: str, tokens: int) -> tuple[str, str]:
        result = await call_claude(prompt, tokens)
        return key, result

    tasks = [_gen(k, p, t) for k, (p, t) in prompts.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    pack: dict[str, str] = {}
    for r in results:
        if isinstance(r, tuple):
            pack[r[0]] = r[1]
    return pack


# ═══════════════════════════════════════════════════════════════════════════════
# MAILCHIMP HTML TEMPLATE
# ═══════════════════════════════════════════════════════════════════════════════

def build_email_html(title: str, url: str, excerpt: str, product: dict) -> str:
    return f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="utf-8"><title>{title}</title></head>
<body style="margin:0;padding:0;background:#0d1117;font-family:system-ui,sans-serif;">
<table width="100%" style="max-width:600px;margin:0 auto;background:#161b22;border-radius:12px;">
<tr><td style="background:linear-gradient(135deg,#0066ff,#00d4ff);padding:28px;text-align:center;">
<h1 style="color:#fff;margin:0;font-size:20px;">📣 Neuer Artikel erschienen</h1>
</td></tr>
<tr><td style="padding:28px;">
<h2 style="color:#e6edf3;margin:0 0 12px;">{title}</h2>
<p style="color:#8b949e;line-height:1.7;">{excerpt[:300]}</p>
<a href="{url}" style="background:#0066ff;color:#fff;padding:12px 24px;border-radius:8px;
   text-decoration:none;font-weight:bold;display:inline-block;margin-top:16px;">→ Jetzt lesen</a>
</td></tr>
<tr style="background:#0d1117;"><td style="padding:20px 28px;">
<p style="color:#8b949e;margin:0 0 8px;font-size:14px;"><strong style="color:#58a6ff;">{product['name']}</strong></p>
<p style="color:#8b949e;font-size:13px;margin:0 0 12px;">{product['pitch']}</p>
<a href="{product['url']}" style="background:#238636;color:#fff;padding:8px 18px;border-radius:6px;
   text-decoration:none;font-size:13px;">Kostenlos testen →</a>
</td></tr>
<tr><td style="padding:16px;text-align:center;">
<p style="color:#8b949e;font-size:11px;margin:0;">
© 2026 BullPower Hub •
<a href="https://bullpower-hub-portal.netlify.app" style="color:#58a6ff;">bullpower-hub-portal.netlify.app</a>
</p></td></tr>
</table>
</body></html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# TASK RUNNERS
# ═══════════════════════════════════════════════════════════════════════════════

async def run_batch_cycle():
    """Full social batch: generate all platform posts, post to Reddit + LinkedIn, send MC campaign."""
    product = random.choice(PRODUCTS)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info(f"Batch cycle: {product['name']}")
    await send_telegram(f"🚀 <b>Social Batch gestartet</b>\n📅 {now_str}\n🎯 {product['name']}")

    # Generate all content in parallel
    reddit_title_task  = asyncio.create_task(gen_reddit(product))
    linkedin_task      = asyncio.create_task(gen_linkedin(product))
    hn_task            = asyncio.create_task(gen_hn(product))
    quora_task         = asyncio.create_task(gen_quora(product))
    pr_task            = asyncio.create_task(gen_pr(product))

    (r_title, r_body), li_text, hn_text, quora_text, pr_text = await asyncio.gather(
        reddit_title_task, linkedin_task, hn_task, quora_task, pr_task,
        return_exceptions=True
    )

    # ── Actually post ──────────────────────────────────────────────────────────
    sub = random.choice(REDDIT_SUBS)
    reddit_ok = await post_to_reddit(
        r_title if isinstance(r_title, str) else product["name"],
        (r_body if isinstance(r_body, str) else "") + f"\n\n{product['url']}",
        sub
    )
    await asyncio.sleep(5)
    li_ok = await post_to_linkedin(
        (li_text if isinstance(li_text, str) else f"{product['name']}\n{product['url']}")
    )

    # ── Mailchimp campaign ─────────────────────────────────────────────────────
    html = build_email_html(
        f"🚀 {product['name']} — Automation Update",
        product["url"],
        product["pitch"],
        product,
    )
    mc_ok = await send_mailchimp_campaign(f"📈 {product['name']} — Neues Update", html)

    # ── Telegram summaries ─────────────────────────────────────────────────────
    for label, content in [
        ("🟠 Reddit", r_body if isinstance(r_body, str) else ""),
        ("💼 LinkedIn", li_text if isinstance(li_text, str) else ""),
        ("🟧 Hacker News", hn_text if isinstance(hn_text, str) else ""),
        ("❓ Quora", quora_text if isinstance(quora_text, str) else ""),
        ("📰 Press Release", pr_text if isinstance(pr_text, str) else ""),
    ]:
        if content:
            await send_telegram(f"{label}\n\n{content[:600]}\n\n🔗 {product['url']}")
            await asyncio.sleep(1)

    # ── Sitemap ping + IndexNow ────────────────────────────────────────────────
    ping_count = await ping_sitemaps()
    await indexnow_ping([product["url"]])

    await klaviyo_track("Social Batch Complete", {
        "product": product["name"],
        "reddit_posted": reddit_ok,
        "linkedin_posted": li_ok,
        "mailchimp_sent": mc_ok,
        "total_posts": _stats["posts_generated"],
    })

    _stats["last_run"] = now_str
    await send_telegram(
        f"✅ <b>Social Batch abgeschlossen</b>\n"
        f"👽 Reddit: {'✅' if reddit_ok else '⏭️'}\n"
        f"💼 LinkedIn: {'✅' if li_ok else '⏭️'}\n"
        f"📧 Mailchimp: {'✅' if mc_ok else '⏭️'}\n"
        f"📡 IndexNow: ✅\n"
        f"🔍 Sitemap pings: {ping_count}\n"
        f"📊 Total posts: {_stats['posts_generated']}"
    )


async def run_reddit_only():
    product = random.choice(PRODUCTS)
    sub = random.choice(REDDIT_SUBS)
    title, body = await gen_reddit(product)
    ok = await post_to_reddit(title, body + f"\n\n{product['url']}", sub)
    await send_telegram(f"👽 <b>Reddit r/{sub}:</b> {'✅' if ok else '⏭️'}\n{title[:80]}")


async def run_linkedin_only():
    product = random.choice(PRODUCTS)
    text = await gen_linkedin(product)
    ok = await post_to_linkedin(text)
    await send_telegram(f"💼 <b>LinkedIn:</b> {'✅' if ok else '⏭️'}\n{text[:200]}")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER — TURBO MODE
# ═══════════════════════════════════════════════════════════════════════════════

async def scheduler_loop():
    await asyncio.sleep(8)
    log.info("⚡ Turbo scheduler started")
    # Immediate startup
    await task_refresh_trends()
    await run_batch_cycle()

    INTERVALS = {
        "batch":     4 * 3600,
        "reddit":    2 * 3600,
        "linkedin":  3 * 3600,
        "trends":    2 * 3600,
        "sitemaps": 6 * 3600,
    }

    while True:
        try:
            if await task_due("batch",    INTERVALS["batch"]):
                await run_batch_cycle()
            if await task_due("reddit",   INTERVALS["reddit"]):
                await run_reddit_only()
            if await task_due("linkedin", INTERVALS["linkedin"]):
                await run_linkedin_only()
            if await task_due("trends",   INTERVALS["trends"]):
                await task_refresh_trends()
            if await task_due("sitemaps", INTERVALS["sitemaps"]):
                await ping_sitemaps()
        except Exception as e:
            log.error(f"Scheduler error: {e}")
            _stats["errors"] += 1
        await asyncio.sleep(120)


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_health(request: web.Request) -> web.Response:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM posts_sent")
        total = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keyword_queue")
        kw_count = (await cursor.fetchone())[0]
    return web.json_response({
        "status": "ok",
        "service": "social-traffic-engine",
        "version": "2.0-TURBO",
        "posts_generated": _stats["posts_generated"],
        "total_db_posts": total,
        "keyword_queue": kw_count,
        "last_run": _stats["last_run"],
        "errors": _stats["errors"],
        "features": ["reddit-live", "linkedin-live", "indexnow",
                     "google-trends", "mailchimp", "content-repurpose"],
    })


async def handle_stats(request: web.Request) -> web.Response:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM posts_sent")
        total = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT COUNT(*) FROM posts_sent WHERE posted_at >= date('now')"
        )
        today = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT platform, COUNT(*) FROM posts_sent GROUP BY platform"
        )
        by_platform = {r[0]: r[1] for r in await cursor.fetchall()}
        cursor = await db.execute("SELECT COUNT(*) FROM keyword_queue")
        kw_count = (await cursor.fetchone())[0]
    return web.json_response({
        "total_posts": total,
        "posts_today": today,
        "by_platform": by_platform,
        "in_memory_by_platform": _stats["posts_by_platform"],
        "last_trends_fetch": _stats["last_trends_fetch"],
        "keyword_queue_size": kw_count,
        "last_run": _stats["last_run"],
        "errors": _stats["errors"],
    })


async def handle_trigger(request: web.Request) -> web.Response:
    asyncio.create_task(run_batch_cycle())
    return web.json_response({"status": "triggered", "task": "batch_cycle"})


async def handle_trigger_batch(request: web.Request) -> web.Response:
    asyncio.create_task(run_batch_cycle())
    return web.json_response({"status": "triggered", "task": "batch"})


async def handle_trigger_reddit(request: web.Request) -> web.Response:
    asyncio.create_task(run_reddit_only())
    return web.json_response({"status": "triggered", "task": "reddit"})


async def handle_trigger_linkedin(request: web.Request) -> web.Response:
    asyncio.create_task(run_linkedin_only())
    return web.json_response({"status": "triggered", "task": "linkedin"})


async def handle_trigger_trending(request: web.Request) -> web.Response:
    asyncio.create_task(task_refresh_trends())
    return web.json_response({"status": "triggered", "task": "trending"})


async def handle_ingest(request: web.Request) -> web.Response:
    """Receive article from SEO Traffic Engine — repurpose into ALL platforms simultaneously."""
    try:
        data = await request.json()
        title   = data.get("title", "")
        url     = data.get("url", "")
        keyword = data.get("keyword", title)
        excerpt = data.get("excerpt", "")
        if not title or not url:
            return web.json_response({"error": "title and url required"}, status=400)
        asyncio.create_task(_process_ingest(title, url, keyword, excerpt))
        return web.json_response({"status": "accepted", "title": title})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def _process_ingest(title: str, url: str, keyword: str, excerpt: str):
    product = random.choice(PRODUCTS)
    log.info(f"Ingest: {title[:60]}")

    # Generate full repurpose pack — ALL platforms in parallel
    pack = await gen_full_repurpose_pack(title, url, keyword, excerpt, product)

    # Post to Reddit
    await post_to_reddit(
        pack.get("reddit_title", title),
        pack.get("reddit_body", excerpt) + f"\n\n{url}",
        random.choice(REDDIT_SUBS),
    )
    await asyncio.sleep(5)

    # Post to LinkedIn
    await post_to_linkedin(pack.get("linkedin", f"{title}\n{url}"))

    # IndexNow for the article URL
    await indexnow_ping([url])

    # Add keyword to queue
    if keyword:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO keyword_queue (keyword, priority, source) VALUES (?, 8, 'ingest')",
                (keyword,)
            )
            await db.commit()

    # Send full pack to Telegram
    tg = (
        f"📰➡️📣 <b>Article Repurposed!</b>\n"
        f"<b>{title[:60]}</b>\n\n"
        f"🐦 <b>Twitter 1:</b> {pack.get('twitter1', '')[:200]}\n\n"
        f"💼 <b>LinkedIn:</b> {pack.get('linkedin', '')[:300]}\n\n"
        f"🎵 <b>TikTok Hook:</b> {pack.get('tiktok', '')[:200]}\n\n"
        f"📌 <b>Pinterest:</b> {pack.get('pinterest', '')[:100]}\n\n"
        f"🔗 {url}"
    )
    await send_telegram(tg)

    # Mailchimp campaign for the article
    html = build_email_html(title, url, excerpt, product)
    subject = pack.get("email_subject", f"📈 Neu: {title[:50]}")
    await send_mailchimp_campaign(subject, html)

    await klaviyo_track("Article Repurposed", {
        "title": title, "url": url, "keyword": keyword,
        "platforms": list(pack.keys()),
    })


async def on_startup(app):
    await init_db()
    asyncio.create_task(scheduler_loop())
    log.info(f"🚀 Social Traffic Engine TURBO v2.0 — Port {PORT}")
    log.info(f"⚡ IndexNow Key: {INDEXNOW_KEY}")


# ═══════════════════════════════════════════════════════════════════════════════
# APP
# ═══════════════════════════════════════════════════════════════════════════════

app = web.Application()
app.on_startup.append(on_startup)

app.router.add_get("/health",                  handle_health)
app.router.add_get("/",                        handle_health)
app.router.add_get("/stats",                   handle_stats)
app.router.add_post("/trigger",                handle_trigger)
app.router.add_post("/api/trigger/batch",      handle_trigger_batch)
app.router.add_post("/api/trigger/reddit",     handle_trigger_reddit)
app.router.add_post("/api/trigger/linkedin",   handle_trigger_linkedin)
app.router.add_post("/api/trigger/trending",   handle_trigger_trending)
app.router.add_post("/api/ingest",             handle_ingest)

if __name__ == "__main__":
    web.run_app(app, port=PORT)
