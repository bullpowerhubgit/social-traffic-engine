#!/usr/bin/env python3
"""
Social Traffic Engine — autonomous Reddit/LinkedIn/HN/Quora content poster
Runs 24/7 on Railway. Sends generated content templates to Telegram.
"""
import asyncio
import logging
import os
import random
import json
from datetime import datetime, timezone
import aiohttp
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("social-traffic-engine")

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PORT          = int(os.getenv("PORT", 8080))
MC_API_KEY    = os.getenv("MAILCHIMP_API_KEY", "")
MC_SERVER     = os.getenv("MAILCHIMP_SERVER_PREFIX", "us7")
MC_LIST_ID    = os.getenv("MAILCHIMP_LIST_ID", "")
KV_API_KEY    = os.getenv("KLAVIYO_API_KEY", "")


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
                }}},
                timeout=aiohttp.ClientTimeout(total=8),
            )
    except Exception:
        pass

PRODUCTS = [
    {"name": "SteuercockPit", "url": "https://bullpower-steuercockpit.netlify.app", "pitch": "KI-Buchhaltung für Selbstständige — Abo-Verwaltung, Bank-CSV-Analyse, ELSTER-Vorbereitung"},
    {"name": "BullPower Hub Bundle", "url": "https://bullpower-hub-portal.netlify.app", "pitch": "8 KI-Automatisierungs-Tools für Shopify + E-Commerce — 14 Tage kostenlos"},
    {"name": "Shopify Acquisition Engine", "url": "https://shopify-acquisition-engine-production.up.railway.app", "pitch": "KI findet profitable Shopify-Produkte automatisch — Trending items, Preisoptimierung"},
    {"name": "Kostenlosen Shopify-Audit", "url": "https://bullpower-lead.netlify.app", "pitch": "KI analysiert deinen Shopify-Shop kostenlos — SEO, Conversion, Preise"},
    {"name": "iComeAuto", "url": "https://bullpower-icomeauto.netlify.app", "pitch": "Income Automation Platform — passives Einkommen vollautomatisch"},
    {"name": "SEO Turbo Tools", "url": "https://seo-turbo-tools-production.up.railway.app", "pitch": "KI-gestützte SEO-Analyse, Keyword Research & Meta-Generator"},
    {"name": "Analytics Marketing Pro", "url": "https://analytics-marketing-pro-production.up.railway.app", "pitch": "Klaviyo, Mailchimp & Facebook Pixel vollautomatisch verbunden"},
    {"name": "Shopify Automaton Suite", "url": "https://shopify-automaton-suite-production-e405.up.railway.app", "pitch": "Vollautomatische Shopify Suite mit Amazon & AliExpress Integration"},
    {"name": "Windsurf Shopify Suite", "url": "https://windsurf-shopify-suite-production.up.railway.app", "pitch": "Shopify SaaS mit KI-Preisoptimierung und automatischer Synchronisation"},
    {"name": "CreatorAI Ultra", "url": "https://creatorai-ultra-production.up.railway.app", "pitch": "KI Content Creator — Blog, Video-Skripte, Social Media Posts vollautomatisch"},
    {"name": "Cognitive Symphony", "url": "https://cognitive-symphony-production.up.railway.app", "pitch": "KI Business Analyse & Automatisierung für E-Commerce Unternehmer"},
    {"name": "Amazon Top-Deals (Affiliate)", "url": "https://www.amazon.de/s?k=Shopify+Automatisierung+Tools&tag=bullpowerhub-21", "pitch": "Top Amazon-Produkte für E-Commerce Automatisierung — Affiliate-Link mit bullpowerhub-21"},
    {"name": "Amazon Gadgets & KI-Tools", "url": "https://www.amazon.de/s?k=KI+Tools+Automatisierung&tag=bullpowerhub-21", "pitch": "KI-Hardware und Gadgets für digitale Unternehmer auf Amazon.de — bullpowerhub-21"},
]

SITEMAPS = [
    "https://shopify-automaton-suite-production-e405.up.railway.app/sitemap.xml",
    "https://seo-turbo-tools-production.up.railway.app/sitemap.xml",
    "https://analytics-marketing-pro-production.up.railway.app/sitemap.xml",
    "https://shopify-acquisition-engine-production.up.railway.app/sitemap.xml",
    "https://bullpower-steuercockpit.netlify.app/sitemap.xml",
    "https://bullpower-hub-portal.netlify.app/sitemap.xml",
    "https://bullpower-lead.netlify.app/sitemap.xml",
]

REDDIT_SUBS = ["r/shopify", "r/ecommerce", "r/SEO", "r/Entrepreneur", "r/smallbusiness", "r/Affiliatemarketing"]
HN_TOPICS = ["Show HN", "Ask HN"]

stats = {"posts_generated": 0, "last_run": None, "errors": 0}


async def call_claude(prompt: str) -> str:
    """Generate content via Claude Haiku."""
    if not ANTHROPIC_KEY:
        return "[ANTHROPIC_KEY fehlt]"
    async with aiohttp.ClientSession() as sess:
        resp = await sess.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 600,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=aiohttp.ClientTimeout(total=30),
        )
        data = await resp.json()
        return data["content"][0]["text"].strip()


async def send_telegram(text: str):
    """Send message to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing")
        return
    async with aiohttp.ClientSession() as sess:
        await sess.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=15),
        )


async def ping_sitemaps():
    """Ping Google + Bing with all project sitemaps for faster indexing."""
    pinged = 0
    async with aiohttp.ClientSession() as sess:
        for sitemap_url in SITEMAPS:
            for engine, endpoint in [
                ("Google", f"https://www.google.com/ping?sitemap={sitemap_url}"),
                ("Bing",   f"https://www.bing.com/ping?sitemap={sitemap_url}"),
            ]:
                try:
                    await sess.get(endpoint, timeout=aiohttp.ClientTimeout(total=8))
                    pinged += 1
                except Exception:
                    pass
    log.info(f"Sitemap ping: {pinged}/{len(SITEMAPS)*2} OK")
    return pinged


async def generate_reddit_post(product: dict) -> str:
    sub = random.choice(REDDIT_SUBS)
    prompt = (
        f"Schreibe einen Reddit-Post für {sub} auf Englisch. Produkt: {product['name']} — {product['pitch']}. "
        f"URL: {product['url']}\n\n"
        "Format: TITLE: (max 120 Zeichen, kein Spam, nützlich) dann BODY: (3-4 Sätze, authentisch, hilfreich, "
        "am Ende die URL natürlich einbinden). Kein Marketing-Speak. Schreibe wie ein echter User der etwas Nützliches teilt."
    )
    content = await call_claude(prompt)
    return f"📝 <b>Reddit Post — {sub}</b>\n\n{content}\n\n🔗 {product['url']}"


async def generate_linkedin_post(product: dict) -> str:
    prompt = (
        f"Schreibe einen professionellen LinkedIn-Post auf Deutsch. Produkt: {product['name']} — {product['pitch']}. "
        f"URL: {product['url']}\n\n"
        "3-5 Zeilen, Business-Kontext, mit 2-3 relevanten Hashtags (#Shopify #Ecommerce #Automatisierung o.ä.), "
        "URL am Ende. Authentisch, kein übertriebenes Marketing."
    )
    content = await call_claude(prompt)
    return f"💼 <b>LinkedIn Post</b>\n\n{content}\n\n🔗 {product['url']}"


async def generate_hn_post(product: dict) -> str:
    prompt = (
        f"Schreibe einen Hacker News 'Show HN' Post auf Englisch. Produkt: {product['name']}. "
        f"Technischer Aspekt: {product['pitch']}. URL: {product['url']}\n\n"
        "Format: TITLE: Show HN: <title> (max 80 Zeichen) dann COMMENT: (2-3 technische Sätze was interessant ist, "
        "warum die HN-Community das interessieren könnte). Technisch, ehrlich, kein Marketing."
    )
    content = await call_claude(prompt)
    return f"🟧 <b>Hacker News</b>\n\n{content}\n\n🔗 {product['url']}"


async def generate_quora_answer(product: dict) -> str:
    questions = [
        "What are the best tools to automate a Shopify store?",
        "How can I increase organic traffic to my e-commerce store?",
        "What SEO tools are worth paying for in 2025?",
        "How do I automate my online business income?",
        "What AI tools help with e-commerce automation?",
    ]
    question = random.choice(questions)
    prompt = (
        f"Schreibe eine hilfreiche Quora-Antwort auf Englisch zur Frage: '{question}'\n"
        f"Erwähne natürlich (nicht als Werbung) {product['name']} — {product['pitch']} mit URL {product['url']}. "
        "3-4 Sätze, zuerst allgemeiner Rat, dann als eine von mehreren Optionen das Produkt erwähnen."
    )
    content = await call_claude(prompt)
    return f"❓ <b>Quora Answer</b>\n<i>Q: {question}</i>\n\n{content}\n\n🔗 {product['url']}"


async def generate_pr_release(product: dict) -> str:
    prompt = (
        f"Schreibe eine kurze Pressemitteilung auf Englisch (für prlog.org, openpr.com). "
        f"Produkt: {product['name']} — {product['pitch']}. URL: {product['url']}. "
        "Headline + 2 Absätze. Professionell, newsworthy. Kein übertriebenes Marketing."
    )
    content = await call_claude(prompt)
    return f"📰 <b>Press Release</b>\n\n{content}\n\n🌐 Submit to: prlog.org | openpr.com | pr.com\n🔗 {product['url']}"


async def run_daily_social_cycle():
    """Full daily content generation cycle."""
    product = random.choice(PRODUCTS)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info(f"Starting social cycle for: {product['name']}")

    header = f"🚀 <b>Social Traffic Engine — Täglicher Content-Batch</b>\n📅 {now}\n🎯 Produkt: {product['name']}\n\n"
    await send_telegram(header)

    generators = [
        ("Reddit", generate_reddit_post),
        ("LinkedIn", generate_linkedin_post),
        ("Hacker News", generate_hn_post),
        ("Quora", generate_quora_answer),
        ("Press Release", generate_pr_release),
    ]

    for platform, gen_func in generators:
        try:
            content = await gen_func(product)
            await send_telegram(content)
            stats["posts_generated"] += 1
            await asyncio.sleep(2)
        except Exception as e:
            log.error(f"{platform} generation failed: {e}")
            stats["errors"] += 1

    # Ping sitemaps for SEO indexing boost
    pinged = await ping_sitemaps()

    stats["last_run"] = now
    await klaviyo_track("Social Content Batch", {
        "product": product["name"],
        "posts_generated": stats["posts_generated"],
        "platforms": ["Reddit", "LinkedIn", "HackerNews", "Quora", "PR"],
    })
    await send_telegram(
        f"✅ <b>Social Batch abgeschlossen</b>\n"
        f"📊 {stats['posts_generated']} Posts generiert\n"
        f"🔍 Sitemaps gepingt: {pinged}/{len(SITEMAPS)*2} (Google + Bing)\n"
        f"📧 Klaviyo getrackt\n"
        f"⏭ Nächster Batch in 8 Stunden"
    )
    log.info("Social cycle complete")


async def run_linkedin_only():
    """LinkedIn-focused cycle every 4 hours."""
    product = random.choice(PRODUCTS)
    try:
        content = await generate_linkedin_post(product)
        await send_telegram(f"💼 <b>Auto-LinkedIn Post</b>\n{content}")
        stats["posts_generated"] += 1
    except Exception as e:
        log.error(f"LinkedIn cycle error: {e}")
        stats["errors"] += 1


async def scheduler_loop():
    """Autonomous scheduling: full batch every 8h, LinkedIn every 4h."""
    # First run immediately
    await asyncio.sleep(10)
    await run_daily_social_cycle()

    cycle = 0
    while True:
        await asyncio.sleep(4 * 3600)  # 4 hours
        cycle += 1
        if cycle % 2 == 0:
            await run_daily_social_cycle()
        else:
            await run_linkedin_only()


# ── Health endpoint ──────────────────────────────────────────────────────────
async def health(request):
    return web.json_response({
        "status": "ok",
        "service": "social-traffic-engine",
        "posts_generated": stats["posts_generated"],
        "last_run": stats["last_run"],
        "errors": stats["errors"],
    })


async def trigger(request):
    """Manual trigger endpoint."""
    asyncio.create_task(run_daily_social_cycle())
    return web.json_response({"status": "triggered", "message": "Social cycle started"})


async def on_startup(app):
    asyncio.create_task(scheduler_loop())


app = web.Application()
async def ingest(request):
    """Receive article from SEO Traffic Engine and generate Reddit + LinkedIn posts."""
    try:
        data = await request.json()
        title = data.get("title", "")
        url = data.get("url", "")
        keyword = data.get("keyword", "")
        excerpt = data.get("excerpt", "")
        if not title or not url:
            return web.json_response({"error": "title and url required"}, status=400)
        asyncio.create_task(_ingest_article(title, url, keyword, excerpt))
        return web.json_response({"status": "accepted", "title": title})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def _ingest_article(title: str, url: str, keyword: str, excerpt: str):
    pseudo_product = {"name": title, "url": url, "pitch": excerpt[:200]}
    reddit_prompt = f"""Reddit post about this article in ENGLISH:
Title: "{title}" | Keyword: {keyword} | URL: {url}
Subreddit suggestion from: r/shopify r/ecommerce r/SEO r/Entrepreneur
Format: Subreddit + Post Title + Body (helpful, not spammy, 150 words max) + link naturally"""
    linkedin_prompt = f"""LinkedIn post auf Deutsch für diesen Artikel:
Titel: "{title}" | Keyword: {keyword} | URL: {url}
Format: Hook + 3 Insights aus dem Artikel + CTA + Link. Max 200 Wörter. Professionell."""
    reddit_content = await call_claude(reddit_prompt)
    linkedin_content = await call_claude(linkedin_prompt)
    stats["posts_generated"] += 2
    await send_telegram(f"📰➡️👽 <b>SEO Artikel → Reddit</b>\n<b>{title[:60]}</b>\n\n{reddit_content[:800]}\n\n🔗 {url}")
    await asyncio.sleep(2)
    await send_telegram(f"📰➡️💼 <b>SEO Artikel → LinkedIn</b>\n<b>{title[:60]}</b>\n\n{linkedin_content[:800]}")


app.router.add_get("/health", health)
app.router.add_post("/trigger", trigger)
app.router.add_post("/api/ingest", ingest)
app.router.add_get("/", health)
app.on_startup.append(on_startup)

if __name__ == "__main__":
    web.run_app(app, port=PORT)
