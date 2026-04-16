# =============================================================================
# main.py  ·  The Babel Brief — 终极完成版
# 核心提升：V2 并发引擎 + V1 全量 RSS 源 + 投行级最严裁决逻辑 + 像素级 HTML 归档
# =============================================================================

import html
import json
import os
import re
import socket
import smtplib
import ssl
import time
import traceback
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parsedate_to_datetime
from typing import Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import feedparser
import markdown
import requests
from dotenv import load_dotenv

# =============================================================================
# § 1  数据模型
# =============================================================================

@dataclass
class NewsItem:
    source: str
    title: str
    summary: str
    link: str
    published: Optional[datetime]
    feed_names: List[str] = field(default_factory=list)

load_dotenv()

APP_USER_AGENT = "The-Babel-Brief/1.0"
ACTIVE_GEMINI_MODEL = ""
LLM_RESPONSE_CACHE: Dict[str, str] = {}
HTTP_SESSION = requests.Session()
HTTP_SESSION.trust_env = os.getenv("ALLOW_ENV_PROXY", "").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_PROMPT_CLUSTER_LIMIT = 20
DEFAULT_PROMPT_ITEMS_PER_CLUSTER = 3
DEFAULT_PROMPT_SUMMARY_LIMIT = 220
LLM_IMPORTANCE_THRESHOLD = 4
DEFAULT_LLM_SCORE_BATCH_SIZE = 18
DEFAULT_SOURCE_CATALOG_LIMIT = 24
TODAY_ECHO_SOURCE_WEIGHTS = {"MusicBrainz": 3, "AllMusic": 3, "Wikidata": 2}
TODAY_ECHO_MIN_CONSENSUS_SCORE = 5
TODAY_ECHO_MIN_SOURCE_COUNT = 2


def http_get(url: str, **kwargs):
    return HTTP_SESSION.get(url, **kwargs)


def http_post(url: str, **kwargs):
    return HTTP_SESSION.post(url, **kwargs)


def http_head(url: str, **kwargs):
    return HTTP_SESSION.head(url, **kwargs)


def probe_http_connectivity(url: str, timeout_s: int = 8) -> Tuple[bool, str]:
    try:
        response = http_get(
            url,
            timeout=timeout_s,
            headers={"User-Agent": APP_USER_AGENT},
        )
        return True, f"{url} -> HTTP {response.status_code}"
    except Exception as exc:
        return False, f"{url} -> {type(exc).__name__}: {exc}"


def probe_tcp_connectivity(host: str, port: int = 443, timeout_s: int = 5) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, ""
    except Exception as exc:
        return False, f"{host}:{port} -> {type(exc).__name__}: {exc}"


def assert_outbound_network_ready() -> None:
    if os.getenv("SKIP_NETWORK_PREFLIGHT", "").strip().lower() in {"1", "true", "yes", "on"}:
        return

    if HTTP_SESSION.trust_env:
        probes = [
            probe_http_connectivity("https://news.google.com", timeout_s=8),
            probe_http_connectivity("https://generativelanguage.googleapis.com", timeout_s=8),
        ]
    else:
        probes = [
            probe_tcp_connectivity("news.google.com", 443),
            probe_tcp_connectivity("generativelanguage.googleapis.com", 443),
        ]
    if any(ok for ok, _ in probes):
        return

    details = "；".join(message for ok, message in probes if not ok)
    raise RuntimeError(f"启动前网络预检失败，当前机器无法建立外网 HTTPS 连接：{details}")

# =============================================================================
# § 2  RSS 源清单（V1 + V2 全量合并，去重取优）
# =============================================================================

DEFAULT_SOURCES: Dict[str, str] = {
    # 通讯社基座
    "Reuters":          "https://news.google.com/rss/search?q=site:reuters.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "Associated Press": "https://news.google.com/rss/search?q=site:apnews.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "AFP":              "https://news.google.com/rss/search?q=site:afp.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    # 全球资本与宏观
    "Bloomberg":        "https://news.google.com/rss/search?q=site:bloomberg.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "WSJ Markets":      "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "Financial Times":  "https://news.google.com/rss/search?q=site:ft.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "The Economist":    "https://www.economist.com/the-world-this-week/rss.xml",
    # 地缘与智库
    "BBC World News":   "https://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera":       "https://www.aljazeera.com/xml/rss/all.xml",
    "Nikkei Asia":      "https://news.google.com/rss/search?q=site:asia.nikkei.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "Tangle News":      "https://news.google.com/rss/search?q=site:readtangle.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    "Foreign Affairs":  "https://news.google.com/rss/search?q=site:foreignaffairs.com+when:24h&hl=en-US&gl=US&ceid=US:en",
    # AI、算力与硬科技
    "Stratechery":      "https://stratechery.com/feed/",
    "Techmeme":         "https://www.techmeme.com/feed.xml",
    "Rest of World":    "https://restofworld.org/feed/",
    "AI & LLM Frontier":"https://news.google.com/rss/search?q=(OpenAI+OR+Anthropic+OR+DeepMind+OR+xAI+OR+LLM+OR+AGI)+(launch+OR+release+OR+breakthrough+OR+investment)+when:24h&hl=en-US&gl=US&ceid=US:en",
    "Tech Macro & Silicon":"https://news.google.com/rss/search?q=(Nvidia+OR+TSMC+OR+Apple+OR+Microsoft+OR+ASML)+(AI+OR+chip+OR+semiconductor+OR+earnings)+when:24h&hl=en-US&gl=US&ceid=US:en",
    "TechCrunch":       "https://techcrunch.com/feed/",
    # 强制涉华
    "South China Morning Post": "https://www.scmp.com/rss/2/feed",
    "Global Macro (China Focus)": "https://news.google.com/rss/search?q=(China+OR+Beijing)+(site:ft.com+OR+site:wsj.com+OR+site:bloomberg.com+OR+site:reuters.com)+when:24h&hl=en-US&gl=US&ceid=US:en",
}

TIER1_SOURCES = {"Reuters", "Associated Press", "AFP", "Bloomberg", "WSJ", "Financial Times"}
PUBLISHER_ALIASES = {
    "reuters": "Reuters",
    "ap": "Associated Press",
    "apnews": "Associated Press",
    "ap news": "Associated Press",
    "associated press": "Associated Press",
    "afp": "AFP",
    "bloomberg": "Bloomberg",
    "financial times": "Financial Times",
    "ft": "Financial Times",
    "wall street journal": "WSJ",
    "wsj": "WSJ",
    "wsj markets": "WSJ",
    "bbc": "BBC World News",
    "bbc news": "BBC World News",
    "al jazeera english": "Al Jazeera",
    "nikkei": "Nikkei Asia",
    "scmp": "South China Morning Post",
    "cointelegraph": "Cointelegraph",
    "the information": "The Information",
    "wall street journal pro": "WSJ",
}

MAJOR_EXCLUSIVE_VETO_PATTERNS = [
    re.compile(r"\b(series [a-z]|funding|fundraise|raises? \$|raised \$|valuation)\b", re.IGNORECASE),
    re.compile(r"\b(partnership|collaboration|joint venture|launch(?:es|ed)?|unveil(?:s|ed)?)\b", re.IGNORECASE),
    re.compile(r"\b(interview|podcast|opinion|analysis|newsletter|preview|hands-on|review)\b", re.IGNORECASE),
    re.compile(r"\b(startup|seed round|venture-backed)\b", re.IGNORECASE),
]

MAJOR_EXCLUSIVE_SYSTEMIC_PATTERNS = [
    (re.compile(r"\b(war|ceasefire|airstrike|missile|drone strike|hostage|military|troops?|evacuation|rescue)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(sanctions?|tariffs?|export controls?|blockade|embargo|nuclear|coup|election|state of emergency)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(fed|federal reserve|ecb|boj|pboc|central bank|rate cut|rate hike|interest rates?)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(default|bankruptcy|nationalization|bailout|debt crisis|currency crisis|bank run)\b", re.IGNORECASE), 4),
]

MAJOR_EXCLUSIVE_SPILLOVER_PATTERNS = [
    re.compile(r"\b(oil|gas|shipping|supply chain|chip|chips|semiconductor|ai infrastructure|cloud|rare earth)\b", re.IGNORECASE),
    re.compile(r"\b(global markets?|stocks?|bonds?|treasuries|currenc(?:y|ies)|inflation|trade flows?)\b", re.IGNORECASE),
    re.compile(r"\b(apple|microsoft|nvidia|tsmc|openai|google|amazon|meta|tesla|saudi|opec|china|iran|russia|ukraine|taiwan|eu)\b", re.IGNORECASE),
]

MAJOR_EXCLUSIVE_ACTION_PATTERNS = [
    re.compile(r"\b(imposes?|approves?|orders?|bans?|halts?|suspends?|deploys?|strikes?|rescues?|cuts?|raises?|defaults?|files?)\b", re.IGNORECASE),
    re.compile(r"\b(agrees?|rejects?|seizes?|targets?|shuts down|restricts?|launches attack|expands?)\b", re.IGNORECASE),
]

CHINA_ENTITY_PATTERNS = [
    re.compile(r"\b(china|beijing|chinese|prc|mainland china)\b", re.IGNORECASE),
    re.compile(r"\b(hong kong|macau|taiwan|taiwan strait|cross-strait)\b", re.IGNORECASE),
    re.compile(r"\b(pbo[c]?|pla|ccp|state council)\b", re.IGNORECASE),
    re.compile(r"\b(byd|huawei|alibaba|tencent|xiaomi|baidu|jd\.com|meituan|temu|shein|catl|smic|lenovo|pinduoduo|deepseek)\b", re.IGNORECASE),
]

CHINA_DIRECT_ACTION_PATTERNS = [
    re.compile(r"\b(china|beijing|chinese|hong kong|taiwan|byd|huawei|alibaba|tencent|xiaomi|baidu|catl|smic|deepseek)\b.{0,60}\b(imposes?|approves?|orders?|bans?|launches?|expands?|invests?|acquires?|exports?|holds?|conducts?|warns?|meets?|talks?|negotiates?|cuts?|raises?)\b", re.IGNORECASE),
    re.compile(r"\b(tariffs?|sanctions?|export controls?|restrictions?|talks?|summit|meeting|visit|deal|drills?)\b.{0,60}\b(china|beijing|chinese|hong kong|taiwan)\b", re.IGNORECASE),
    re.compile(r"\b(us[- ]china|u\.s\.-china|china[- ]us|eu[- ]china|china[- ]eu|cross[- ]strait|taiwan strait|south china sea)\b", re.IGNORECASE),
]

CHINA_POLICY_TOPIC_PATTERNS = [
    re.compile(r"\b(tariffs?|sanctions?|export controls?|duties|trade talks?|policy|stimulus|rate cut|rate hike|property rescue|industrial policy)\b", re.IGNORECASE),
    re.compile(r"\b(diplomacy|military drills?|naval|semiconductor|chips?|ai exports?|ev exports?|rare earths?)\b", re.IGNORECASE),
    re.compile(r"\b(outbound expansion|overseas expansion|factory in|investment in|listing in|ipo|acquisition)\b", re.IGNORECASE),
]

CHINA_BACKGROUND_ONLY_PATTERNS = [
    re.compile(r"\b(competition from china|pressure from chinese rivals|amid chinese competition)\b", re.IGNORECASE),
    re.compile(r"\b(due to weak demand in china|weak demand in china|china slowdown|slowdown in china)\b", re.IGNORECASE),
    re.compile(r"\b(china market weakness|sales in china|exposure to china|reliance on china)\b", re.IGNORECASE),
    re.compile(r"\b(affected by china|because of china|concerns about china|linked to china demand)\b", re.IGNORECASE),
]

DEEP_SECTION_TITLES = {
    "## 🇨🇳【中国与世界 / China & The World】",
    "## 🌍【全球局势 / Global Affairs】",
    "## 📈【商业与市场 / Business & Markets】",
    "## 🚀【科技与AI / Tech & AI】",
}

QUICK_HITS_TITLE = "## 【Quick Hits】"
QUICK_HITS_MAX_TOTAL = 12
QUICK_HITS_MAX_EXCLUSIVE = 2

SECTION_MIN_ITEMS = 3
SECTION_MAX_ITEMS = 6
SECTION_EXCEPTIONAL_MAX_ITEMS = 8
SECTION_EXCEPTIONAL_SCORE = 11
SECTION_DEFAULT_ITEMS = 3
SECTION_SOFT_MAX_ITEMS = 4
SECTION_SOFT_EXTRA_LLM_SCORE = 7
SECTION_EXCEPTIONAL_LLM_SCORE = 8
PRIMARY_SOURCE_CAP_DEFAULT = 2
PRIMARY_FEED_CAP_DEFAULT = 2
PRIMARY_SOURCE_CAPS = {
    "Techmeme": 1,
}
PRIMARY_FEED_CAPS = {
    "Techmeme": 1,
}

SOURCE_NAME_HINTS = {
    "apnews.com": "Associated Press",
    "bbc.com": "BBC World News",
    "aljazeera.com": "Al Jazeera",
    "reuters.com": "Reuters",
    "bloomberg.com": "Bloomberg",
    "ft.com": "Financial Times",
    "wsj.com": "WSJ",
    "wsj.net": "WSJ",
    "stratechery.com": "Stratechery",
    "techmeme.com": "Techmeme",
    "techcrunch.com": "TechCrunch",
    "restofworld.org": "Rest of World",
    "economist.com": "The Economist",
    "scmp.com": "South China Morning Post",
    "nikkei.com": "Nikkei Asia",
    "asia.nikkei.com": "Nikkei Asia",
    "wired.com": "Wired",
    "cointelegraph.com": "Cointelegraph",
    "theinformation.com": "The Information",
    "foreignaffairs.com": "Foreign Affairs",
    "readtangle.com": "Tangle News",
}

HEADLINE_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "amid", "after", "over", "into", "will",
    "says", "say", "said", "plan", "plans", "news", "amid", "still", "more", "than", "near",
    "over", "under", "into", "about", "against", "ahead", "their", "they", "have", "has", "had",
    "are", "was", "were", "but", "not", "new", "its", "his", "her", "our", "your", "who", "why",
    "how", "what", "when", "where", "while", "during", "could", "would", "should", "may",
    "market", "markets", "report", "reports", "sources", "source", "sourc",
}

GLOBAL_AFFAIRS_PATTERNS = [
    (re.compile(r"\b(war|ceasefire|airstrike|missile|drone|military|troops?|hostage|rescue|evacuation|sanctions?|tariffs?|export controls?|blockade|embargo)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(election|vote|parliament|congress|diplom(?:acy|atic)|summit|meeting|talks?|visit|treaty|coup|state of emergency)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(iran|israel|gaza|ukraine|russia|china|taiwan|south china sea|eu|nato|united nations|white house)\b", re.IGNORECASE), 2),
]

BUSINESS_MARKETS_PATTERNS = [
    (re.compile(r"\b(rate cut|rate hike|interest rates?|inflation|jobs report|payrolls|gdp|cpi|ppi|pmi|recession|central bank|fed|ecb|boj|pboc)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(stocks?|bonds?|treasuries|equities|markets?|index|currency|currencies|fx|oil prices?|earnings|profit|revenue|forecast|ipo|m&a|acquisition|merger|bankruptcy|default)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(bank|payment|payments|credit|loan|debt|funding|shipping|supply chain|commodities|crypto)\b", re.IGNORECASE), 2),
]

TECH_AI_PATTERNS = [
    (re.compile(r"\b(openai|anthropic|deepmind|xai|meta ai|gemini|chatgpt|claude|llm|large language model|foundation model|superintelligence|ai model)\b", re.IGNORECASE), 4),
    (re.compile(r"\b(nvidia|tsmc|asml|intel|semiconductor|chips?|gpu|datacenter|cloud|advanced packaging|ai infrastructure)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(stratechery|artificial intelligence|machine learning|agentic|inference|training)\b", re.IGNORECASE), 2),
]

TECH_AI_BLACKLIST_PATTERNS = [
    re.compile(r"\b(review|hands-on|smartphone|laptop|camera|gadget|accessory|linux distro|raspberry pi|diy)\b", re.IGNORECASE),
    re.compile(r"\b(drone strike|missile|warship|mine|torpedo|fighter jet)\b", re.IGNORECASE),
]

LEADING_DECORATION_RE = re.compile("^[\\s\\u2600-\\u27BF\\U0001F1E6-\\U0001F1FF\\U0001F300-\\U0001FAFF\\uFE0F]+")

# =============================================================================
# § 3  抓取引擎 (并发 + URL 解析 + 智能来源推断)
# =============================================================================

def clean_text(text) -> str:
    if not text: return ""
    value = html.unescape(str(text)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value)).strip()


def get_local_now() -> datetime:
    timezone_name = (
        os.getenv("BRIEF_TIMEZONE", "").strip()
        or os.getenv("TZ", "").strip()
    )
    if timezone_name:
        try:
            return datetime.now(ZoneInfo(timezone_name))
        except Exception:
            pass
    return datetime.now().astimezone()


def get_today_month_day() -> str:
    return get_local_now().strftime("%m-%d")


def using_explicit_gemini_model() -> bool:
    return bool(
        (
            os.getenv("LLM_MODEL")
            or os.getenv("GOOGLE_MODEL")
            or os.getenv("GEMINI_MODEL")
            or ""
        ).strip()
    )


def should_avoid_heavy_llm_repairs() -> bool:
    explicit = (
        os.getenv("LLM_MODEL")
        or os.getenv("GOOGLE_MODEL")
        or os.getenv("GEMINI_MODEL")
        or ""
    ).strip()
    return explicit == "gemini-3-flash-preview"


def get_prompt_cluster_limit() -> int:
    return max(8, int(os.getenv("MAX_PROMPT_CLUSTERS", str(DEFAULT_PROMPT_CLUSTER_LIMIT))))


def get_prompt_items_per_cluster() -> int:
    return max(1, int(os.getenv("MAX_PROMPT_ITEMS_PER_CLUSTER", str(DEFAULT_PROMPT_ITEMS_PER_CLUSTER))))


def get_prompt_summary_limit() -> int:
    return max(80, int(os.getenv("PROMPT_SUMMARY_LIMIT", str(DEFAULT_PROMPT_SUMMARY_LIMIT))))


def get_llm_score_batch_size() -> int:
    return max(4, int(os.getenv("LLM_SCORE_BATCH_SIZE", str(DEFAULT_LLM_SCORE_BATCH_SIZE))))


def get_source_catalog_limit() -> int:
    configured = int(os.getenv("MAX_SOURCE_CATALOG_CLUSTERS", str(DEFAULT_SOURCE_CATALOG_LIMIT)))
    floor = 16 if should_avoid_heavy_llm_repairs() else 20
    return max(floor, configured)


def get_today_echo_timeout(parent_timeout_s: int) -> int:
    configured = int(os.getenv("TODAY_ECHO_HTTP_TIMEOUT_SECONDS", "15"))
    return max(6, min(parent_timeout_s, configured))

def resolve_url(url: str, timeout_s: int = 8) -> str:
    if "news.google.com/rss/articles/" not in url: return url
    try:
        resp = http_head(url, allow_redirects=True, timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
        return resp.url if resp.url else url
    except: return url


def infer_source_name_from_url(url: str) -> str:
    host = urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
    if host in {"news.google.com", "news.googleusercontent.com"}:
        return "Source"
    for domain, name in SOURCE_NAME_HINTS.items():
        if domain in host:
            return name
    return host or "Source"


def normalize_publisher_token(token: str) -> str:
    value = clean_text(token)
    value = re.sub(r"^[Ss]ource[:：]\s*", "", value)
    value = re.sub(r"\.com$", "", value, flags=re.IGNORECASE)
    value = value.strip(" -|—–:;,.")
    return canonicalize_source_name(value) if value else ""


def looks_like_known_publisher(token: str) -> bool:
    normalized = normalize_publisher_token(token)
    if not normalized:
        return False
    known = set(PUBLISHER_ALIASES.values()) | set(DEFAULT_SOURCES.keys()) | set(SOURCE_NAME_HINTS.values())
    known.update({"CNBC", "The Information", "Wired", "2 Minute Medicine"})
    return normalized in known


def infer_publisher_from_title_tail(title: str) -> str:
    value = clean_text(title)
    match = re.search(r"\(([^()]+)\)\s*$", value)
    if match:
        tail = match.group(1).strip()
        candidate = tail.split("/")[-1].strip() if "/" in tail else tail
        normalized = normalize_publisher_token(candidate)
        if looks_like_known_publisher(candidate):
            return normalized

    match = re.search(r"\s(?:[-—–|:])\s*([A-Za-z0-9 .&/-]{2,80})\s*$", value)
    if not match:
        return ""
    tail = match.group(1).strip()
    normalized = normalize_publisher_token(tail)
    if looks_like_known_publisher(tail):
        return normalized
    return ""


def normalize_news_title(title: str, source_hint: str = "") -> str:
    value = clean_text(title)
    if not value:
        return ""

    canonical_hint = normalize_publisher_token(source_hint)
    prefix_match = re.match(r"^[^:]{1,120}/\s*([^:]{2,60})\s*:\s*(.+)$", value)
    if prefix_match:
        prefix_source = normalize_publisher_token(prefix_match.group(1))
        if prefix_source and (not canonical_hint or prefix_source == canonical_hint or looks_like_known_publisher(prefix_match.group(1))):
            value = prefix_match.group(2).strip()

    paren_match = re.match(r"^(.*)\(([^()]+)\)\s*$", value)
    if paren_match:
        tail = paren_match.group(2).split("/")[-1].strip()
        normalized_tail = normalize_publisher_token(tail)
        if normalized_tail and (normalized_tail == canonical_hint or looks_like_known_publisher(tail)):
            value = paren_match.group(1).strip()

    sep_match = re.match(r"^(.*?)(?:\s[-—–|]\s)([^-—–|]{2,80})$", value)
    if sep_match:
        tail = sep_match.group(2).strip()
        normalized_tail = normalize_publisher_token(tail)
        if normalized_tail and (normalized_tail == canonical_hint or looks_like_known_publisher(tail)):
            value = sep_match.group(1).strip()

    return clean_text(value).rstrip(" -|—–:;,.")


def infer_item_source(feed_name: str, title: str, link: str) -> str:
    if feed_name == "Techmeme":
        tail_source = infer_publisher_from_title_tail(title)
        if tail_source:
            return tail_source

    url_source = canonicalize_source_name(infer_source_name_from_url(link))
    if url_source not in {"Source", "techmeme.com", "news.google.com", "news.googleusercontent.com"}:
        return url_source

    tail_source = infer_publisher_from_title_tail(title)
    if tail_source and tail_source not in {"Source", "Techmeme"}:
        return tail_source

    return canonicalize_source_name(feed_name)

def fetch_rss(feed_name: str, url: str, timeout_s: int) -> List[NewsItem]:
    items = []
    try:
        res = http_get(url, timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
        feed = feedparser.parse(res.content)
        for entry in getattr(feed, "entries", [])[:8]:
            pub_date = parse_datetime(entry)
            if pub_date and (datetime.now(timezone.utc) - pub_date).total_seconds() > 86400: continue
            
            raw_title = clean_text(getattr(entry, "title", ""))
            link = resolve_url(getattr(entry, "link", ""))
            source = infer_item_source(feed_name, raw_title, link)
            title = normalize_news_title(raw_title, source)
            items.append(NewsItem(
                source=source,
                title=title,
                summary=clean_text(getattr(entry, "summary", ""))[:320],
                link=link,
                published=pub_date,
                feed_names=[feed_name]
            ))
    except Exception as e: print(f"[WARN] {feed_name} 失败: {e}")
    return items

def parse_datetime(entry) -> Optional[datetime]:
    parsed = getattr(entry, "published_parsed", None)
    return datetime(*parsed[:6], tzinfo=timezone.utc) if parsed else None

# =============================================================================
# § 4  裁决算法 (语义去重与聚类)
# =============================================================================

def headline_tokens(text: str) -> List[str]:
    normalized = normalize_news_title(clean_text(text))
    tokens = re.findall(r"[A-Za-z0-9]+", normalized.lower())
    return [
        token
        for token in tokens
        if len(token) >= 3 and token not in HEADLINE_STOPWORDS and token not in {name.lower() for name in PUBLISHER_ALIASES.values()}
    ]


def titles_match(t1: str, t2: str) -> bool:
    left = clean_text(t1).lower()
    right = clean_text(t2).lower()
    if not left or not right:
        return False
    ratio = SequenceMatcher(None, left, right).ratio()
    if ratio > 0.82:
        return True

    left_tokens = set(headline_tokens(left))
    right_tokens = set(headline_tokens(right))
    if not left_tokens or not right_tokens:
        return False

    overlap = left_tokens & right_tokens
    if len(overlap) >= 5:
        return True
    if len(overlap) >= 4 and len(overlap) / max(1, min(len(left_tokens), len(right_tokens))) >= 0.6:
        return True
    if len(overlap) >= 4 and ratio >= 0.48:
        return True
    if len(overlap) >= 5 and len(overlap) / max(1, len(left_tokens | right_tokens)) >= 0.45:
        return True
    return False


def cluster_matches_item(cluster: Sequence[NewsItem], item: NewsItem) -> bool:
    return any(
        titles_match(item.title, existing.title)
        or (
            clean_text(item.summary)
            and clean_text(existing.summary)
            and titles_match(item.summary, existing.summary)
        )
        for existing in cluster
    )


def cluster_items(items: List[NewsItem]) -> List[List[NewsItem]]:
    clusters: List[List[NewsItem]] = []
    for item in items:
        matched = False
        for c in clusters:
            if cluster_matches_item(c, item):
                c.append(item); matched = True; break
        if not matched: clusters.append([item])
    return [c for c in clusters if len(c) > 0]


def canonicalize_source_name(source: str) -> str:
    value = clean_label_text(source) if source else ""
    lower = value.lower()
    return PUBLISHER_ALIASES.get(lower, value)


def is_tier1_source(source: str) -> bool:
    return canonicalize_source_name(source) in TIER1_SOURCES


def score_major_exclusive(cluster: List[NewsItem]) -> Dict[str, object]:
    primary = cluster[0]
    source = canonicalize_source_name(primary.source)
    corpus = " ".join(
        clean_text(part)
        for item in cluster
        for part in (item.title, item.summary)
        if part
    )
    lower_corpus = corpus.lower()

    veto_hits = [pattern.pattern for pattern in MAJOR_EXCLUSIVE_VETO_PATTERNS if pattern.search(lower_corpus)]
    source_score = 2 if is_tier1_source(source) else 0

    impact_score = 0
    for pattern, score in MAJOR_EXCLUSIVE_SYSTEMIC_PATTERNS:
        if pattern.search(lower_corpus):
            impact_score = max(impact_score, score)

    spillover_score = min(2, sum(1 for pattern in MAJOR_EXCLUSIVE_SPILLOVER_PATTERNS if pattern.search(lower_corpus)))
    action_score = min(2, sum(1 for pattern in MAJOR_EXCLUSIVE_ACTION_PATTERNS if pattern.search(lower_corpus)))

    total_score = source_score + impact_score + spillover_score + action_score
    eligible = source_score >= 2 and impact_score >= 4 and total_score >= 8 and not veto_hits

    return {
        "source_score": source_score,
        "impact_score": impact_score,
        "spillover_score": spillover_score,
        "action_score": action_score,
        "total_score": total_score,
        "threshold": 8,
        "eligible": eligible,
        "veto_hits": veto_hits,
    }


def score_china_focus(cluster: List[NewsItem]) -> Dict[str, object]:
    primary = cluster[0]
    title = clean_text(primary.title)
    title_lower = title.lower()
    corpus = " ".join(
        clean_text(part)
        for item in cluster
        for part in (item.title, item.summary)
        if part
    )
    lower_corpus = corpus.lower()

    title_entity_hits = sum(1 for pattern in CHINA_ENTITY_PATTERNS if pattern.search(title_lower))
    corpus_entity_hits = sum(1 for pattern in CHINA_ENTITY_PATTERNS if pattern.search(lower_corpus))
    direct_action_hits = sum(1 for pattern in CHINA_DIRECT_ACTION_PATTERNS if pattern.search(lower_corpus))
    policy_topic_hits = sum(1 for pattern in CHINA_POLICY_TOPIC_PATTERNS if pattern.search(lower_corpus))
    background_hits = [pattern.pattern for pattern in CHINA_BACKGROUND_ONLY_PATTERNS if pattern.search(lower_corpus)]

    entity_score = 3 if title_entity_hits else 2 if corpus_entity_hits else 0
    role_score = 3 if direct_action_hits >= 2 else 2 if direct_action_hits == 1 else 0
    topic_score = 2 if policy_topic_hits >= 2 else 1 if policy_topic_hits == 1 else 0
    penalty = 4 if background_hits else 0

    total_score = entity_score + role_score + topic_score - penalty
    eligible = entity_score >= 2 and role_score >= 2 and total_score >= 5 and not background_hits

    return {
        "entity_score": entity_score,
        "role_score": role_score,
        "topic_score": topic_score,
        "background_penalty": penalty,
        "total_score": total_score,
        "threshold": 5,
        "eligible": eligible,
        "background_hits": background_hits,
        "background_only": bool(background_hits),
    }


def build_cluster_corpus(cluster: List[NewsItem]) -> str:
    return " ".join(
        clean_text(part)
        for item in cluster
        for part in (item.title, item.summary)
        if part
    )


def count_tier1_sources(items: Sequence[NewsItem]) -> int:
    return len({canonicalize_source_name(item.source) for item in items if is_tier1_source(item.source)})


def recency_score(published: Optional[datetime]) -> int:
    if not published:
        return 0
    hours = (datetime.now(timezone.utc) - published).total_seconds() / 3600
    if hours <= 6:
        return 2
    if hours <= 12:
        return 1
    return 0


def weighted_pattern_score(text: str, patterns: Sequence[Tuple[re.Pattern, int]]) -> int:
    return sum(weight for pattern, weight in patterns if pattern.search(text))


def score_section_candidate(cluster: List[NewsItem], section: str) -> Dict[str, object]:
    sorted_cluster = sorted(
        cluster,
        key=lambda item: item.published or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    primary = sorted_cluster[0]
    lower_corpus = build_cluster_corpus(sorted_cluster).lower()
    source_count = len({canonicalize_source_name(item.source) for item in sorted_cluster})
    tier1_count = count_tier1_sources(sorted_cluster)
    consensus_bonus = min(4, source_count * 2) if source_count >= 2 else 0
    tier1_bonus = min(3, tier1_count)
    freshness_bonus = recency_score(primary.published)
    major_bonus = 2 if source_count == 1 and score_major_exclusive(sorted_cluster).get("eligible") else 0
    china_score = score_china_focus(sorted_cluster)

    if section == "china":
        section_fit = china_score["total_score"] + (2 if china_score["eligible"] else 0)
        eligible = china_score["eligible"]
    elif section == "global":
        section_fit = weighted_pattern_score(lower_corpus, GLOBAL_AFFAIRS_PATTERNS)
        eligible = section_fit >= 4
    elif section == "business":
        section_fit = weighted_pattern_score(lower_corpus, BUSINESS_MARKETS_PATTERNS)
        eligible = section_fit >= 4
    elif section == "tech":
        blacklist_hits = any(pattern.search(lower_corpus) for pattern in TECH_AI_BLACKLIST_PATTERNS)
        section_fit = weighted_pattern_score(lower_corpus, TECH_AI_PATTERNS)
        eligible = section_fit >= 4 and not blacklist_hits
        if blacklist_hits:
            section_fit = 0
    else:
        section_fit = 0
        eligible = False

    total_score = section_fit + consensus_bonus + tier1_bonus + freshness_bonus + major_bonus

    return {
        "section": section,
        "section_fit": section_fit,
        "consensus_bonus": consensus_bonus,
        "tier1_bonus": tier1_bonus,
        "freshness_bonus": freshness_bonus,
        "major_bonus": major_bonus,
        "total_score": total_score,
        "eligible": eligible,
    }


def cluster_priority_key(cluster: List[NewsItem]) -> Tuple[float, float, float, float]:
    sorted_cluster = sorted(
        cluster,
        key=lambda item: item.published or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    quick_hits_score = score_quick_hits_consensus(sorted_cluster).get("total_score", 0)
    major_score = score_major_exclusive(sorted_cluster).get("total_score", 0) if len({
        canonicalize_source_name(item.source) for item in sorted_cluster
    }) == 1 else 0
    section_peak = max(
        score_section_candidate(sorted_cluster, section).get("total_score", 0)
        for section in ("china", "global", "business", "tech")
    )
    source_diversity = len({canonicalize_source_name(item.source) for item in sorted_cluster})
    freshness = recency_score(sorted_cluster[0].published)
    return (
        max(quick_hits_score, major_score, section_peak),
        quick_hits_score + section_peak,
        source_diversity,
        freshness,
    )


def source_cap_for(name: str, cap_map: Dict[str, int], default_cap: int) -> int:
    return cap_map.get(canonicalize_source_name(name), default_cap)


def get_score_value(candidate: Dict[str, object], score_key: str) -> Dict[str, object]:
    if "." not in score_key:
        return candidate.get(score_key, {}) or {}

    current: object = candidate
    for part in score_key.split("."):
        if not isinstance(current, dict):
            return {}
        current = current.get(part, {})
    return current if isinstance(current, dict) else {}


def get_llm_importance_score(candidate: Dict[str, object]) -> int:
    return int(candidate.get("llm_importance_score", {}).get("score", -1))


def candidate_title_variants(candidate: Dict[str, object], limit: int = 6) -> List[str]:
    variants: List[str] = []
    for raw in [candidate.get("headline", "")] + [item.get("title", "") for item in candidate.get("items", [])]:
        title = clean_text(raw)
        if not title:
            continue
        if any(titles_match(title, existing) or title in existing or existing in title for existing in variants):
            continue
        variants.append(title)
        if len(variants) >= limit:
            break
    return variants


def candidate_link_set(candidate: Dict[str, object]) -> set:
    return {
        clean_text(item.get("link", ""))
        for item in candidate.get("items", [])
        if clean_text(item.get("link", ""))
    }


def candidates_match(left: Dict[str, object], right: Dict[str, object]) -> bool:
    left_links = candidate_link_set(left)
    right_links = candidate_link_set(right)
    if left_links and right_links and left_links & right_links:
        return True

    for left_title in candidate_title_variants(left):
        for right_title in candidate_title_variants(right):
            if (
                titles_match(left_title, right_title)
                or left_title in right_title
                or right_title in left_title
            ):
                return True
    return False


def rank_candidates(candidates: Sequence[Dict[str, object]], score_key: str) -> List[Dict[str, object]]:
    return sorted(
        candidates,
        key=lambda item: (
            get_llm_importance_score(item),
            get_score_value(item, score_key).get("total_score", 0),
            item.get("source_count", 0),
            item.get("items", [{}])[0].get("published") or "",
        ),
        reverse=True,
    )


def select_ranked_candidates(
    candidates: Sequence[Dict[str, object]],
    score_key: str,
    min_items: int,
    max_items: int,
) -> List[Dict[str, object]]:
    ranked = rank_candidates(candidates, score_key)

    selected: List[Dict[str, object]] = []
    primary_source_counts: Dict[str, int] = defaultdict(int)
    primary_feed_counts: Dict[str, int] = defaultdict(int)

    for candidate in ranked:
        if any(candidates_match(candidate, existing) for existing in selected):
            continue

        primary_source = canonicalize_source_name(candidate.get("primary_source", ""))
        primary_feed = clean_text(candidate.get("primary_feed", ""))
        source_cap = source_cap_for(primary_source, PRIMARY_SOURCE_CAPS, PRIMARY_SOURCE_CAP_DEFAULT)
        feed_cap = PRIMARY_FEED_CAPS.get(primary_feed, PRIMARY_FEED_CAP_DEFAULT)

        if primary_source and primary_source_counts[primary_source] >= source_cap:
            continue
        if primary_feed and primary_feed_counts[primary_feed] >= feed_cap:
            continue

        selected.append(candidate)
        if primary_source:
            primary_source_counts[primary_source] += 1
        if primary_feed:
            primary_feed_counts[primary_feed] += 1

        exceptional_count = sum(
            1
            for item in selected
            if get_score_value(item, score_key).get("total_score", 0) >= SECTION_EXCEPTIONAL_SCORE
        )
        dynamic_cap = max_items
        if max_items == SECTION_MAX_ITEMS and exceptional_count >= 2:
            dynamic_cap = SECTION_EXCEPTIONAL_MAX_ITEMS
        if len(selected) >= dynamic_cap:
            return selected

    if len(selected) >= min_items:
        return selected

    for candidate in ranked:
        if any(candidates_match(candidate, existing) for existing in selected):
            continue
        selected.append(candidate)
        if len(selected) >= min(max_items, min_items):
            break

    return selected


def select_section_candidates(candidates: Sequence[Dict[str, object]], score_key: str) -> List[Dict[str, object]]:
    ranked = select_ranked_candidates(candidates, score_key, SECTION_MIN_ITEMS, SECTION_EXCEPTIONAL_MAX_ITEMS)
    if len(ranked) <= SECTION_DEFAULT_ITEMS:
        return ranked

    selected = ranked[:SECTION_DEFAULT_ITEMS]
    for index, candidate in enumerate(ranked[SECTION_DEFAULT_ITEMS:], start=SECTION_DEFAULT_ITEMS + 1):
        llm_score = get_llm_importance_score(candidate)
        section_score = get_score_value(candidate, score_key).get("total_score", 0)
        allow = False
        if index == SECTION_DEFAULT_ITEMS + 1:
            allow = llm_score >= SECTION_SOFT_EXTRA_LLM_SCORE or section_score >= SECTION_EXCEPTIONAL_SCORE - 1
        else:
            allow = llm_score >= SECTION_EXCEPTIONAL_LLM_SCORE or section_score >= SECTION_EXCEPTIONAL_SCORE
        if not allow:
            break
        selected.append(candidate)
    return selected


SECTION_SELECTION_KEY_MAP = {
    "china": "china_focus_candidates",
    "global": "global_affairs_candidates",
    "business": "business_market_candidates",
    "tech": "tech_ai_candidates",
}

SECTION_SCORE_KEY_MAP = {
    "china": "section_scores.china",
    "global": "section_scores.global",
    "business": "section_scores.business",
    "tech": "section_scores.tech",
}


def candidate_can_fill_section(candidate: Dict[str, object], section_name: str) -> bool:
    score = get_score_value(candidate, SECTION_SCORE_KEY_MAP[section_name])
    if score.get("section_fit", 0) <= 0:
        return False
    if section_name == "china" and candidate.get("china_focus_score", {}).get("background_only"):
        return False
    return True


def section_fallback_candidates(
    raw_clusters: Sequence[Dict[str, object]],
    section_name: str,
    existing: Sequence[Dict[str, object]],
    limit: int = SECTION_DEFAULT_ITEMS,
) -> List[Dict[str, object]]:
    fallback: List[Dict[str, object]] = []
    score_key = SECTION_SCORE_KEY_MAP[section_name]
    for candidate in rank_candidates(raw_clusters, score_key):
        if not candidate_can_fill_section(candidate, section_name):
            continue
        if any(candidates_match(candidate, item) for item in existing):
            continue
        if any(candidates_match(candidate, item) for item in fallback):
            continue
        score = get_score_value(candidate, score_key)
        llm_score = get_llm_importance_score(candidate)
        if llm_score < LLM_IMPORTANCE_THRESHOLD and score.get("total_score", 0) < SECTION_EXCEPTIONAL_SCORE - 1:
            continue
        candidate["assigned_section"] = section_name
        fallback.append(candidate)
        if len(fallback) >= limit:
            break
    return fallback


def build_news_items_from_serialized_candidates(candidates: Sequence[Dict[str, object]]) -> List[NewsItem]:
    news_items: List[NewsItem] = []
    seen = set()
    for candidate in candidates:
        for item in candidate.get("items", []):
            title = clean_text(item.get("title", ""))
            link = clean_text(item.get("link", ""))
            key = link or title
            if not key or key in seen:
                continue
            published_raw = clean_text(item.get("published", ""))
            published = None
            if published_raw:
                try:
                    published = datetime.fromisoformat(published_raw)
                except ValueError:
                    published = None
            news_items.append(
                NewsItem(
                    source=canonicalize_source_name(str(item.get("source", ""))),
                    title=title,
                    summary=clean_text(item.get("summary", "")),
                    link=link,
                    published=published,
                    feed_names=list(item.get("feed_names", [])),
                )
            )
            seen.add(key)
    return news_items


def merge_serialized_candidate_group(candidates: Sequence[Dict[str, object]]) -> Dict[str, object]:
    news_items = build_news_items_from_serialized_candidates(candidates)
    merged = serialize_cluster(
        news_items,
        max_items=max(3, get_prompt_items_per_cluster()),
        summary_limit=max(180, get_prompt_summary_limit()),
    )
    top_llm_score = max((get_llm_importance_score(candidate) for candidate in candidates), default=0)
    merged["llm_importance_score"] = {
        "score": top_llm_score,
        "threshold": LLM_IMPORTANCE_THRESHOLD,
        "eligible": top_llm_score > LLM_IMPORTANCE_THRESHOLD,
    }
    return merged


def dedupe_similar_candidates(
    candidates: Sequence[Dict[str, object]],
    score_key: str,
) -> List[Dict[str, object]]:
    unique: List[Dict[str, object]] = []
    for candidate in rank_candidates(candidates, score_key):
        if any(candidates_match(candidate, existing) for existing in unique):
            continue
        unique.append(candidate)
    return unique


def build_quick_hits_consensus_candidates(candidates: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    groups: List[List[Dict[str, object]]] = []
    for candidate in rank_candidates(candidates, "quick_hits_score"):
        if candidate.get("source_count", 0) >= 2:
            groups.append([candidate])
            continue

        placed = False
        for group in groups:
            if any(candidates_match(candidate, existing) for existing in group):
                group.append(candidate)
                placed = True
                break
        if not placed:
            groups.append([candidate])

    merged_candidates: List[Dict[str, object]] = []
    for group in groups:
        merged = group[0] if len(group) == 1 else merge_serialized_candidate_group(group)
        if merged.get("source_count", 0) >= 2:
            merged_candidates.append(merged)
    return dedupe_similar_candidates(merged_candidates, "quick_hits_score")


def rebalance_section_candidates(
    selected_map: Dict[str, List[Dict[str, object]]],
    section_pools: Dict[str, List[Dict[str, object]]],
) -> Dict[str, List[Dict[str, object]]]:
    section_order = ["china", "global", "business", "tech"]

    def selected_identity_map() -> Dict[str, str]:
        identities: Dict[str, str] = {}
        for section_name, candidates in selected_map.items():
            for candidate in candidates:
                identity = candidate_identity(candidate)
                if identity:
                    identities[identity] = section_name
        return identities

    for section_name in section_order:
        identities = selected_identity_map()
        ranked_pool = rank_candidates(section_pools[section_name], SECTION_SCORE_KEY_MAP[section_name])
        for candidate in ranked_pool:
            if len(selected_map[section_name]) >= SECTION_MIN_ITEMS:
                break
            identity = candidate_identity(candidate)
            if not identity or identity in identities:
                continue
            selected_map[section_name].append(candidate)
            identities[identity] = section_name

    while True:
        underfilled = next(
            (
                section_name
                for section_name in section_order
                if len(selected_map[section_name]) < min(SECTION_MIN_ITEMS, len(section_pools[section_name]))
            ),
            "",
        )
        if not underfilled:
            break

        best_move: Optional[Tuple[Tuple[float, int, int, str], str, Dict[str, object]]] = None
        for donor in section_order:
            if donor == underfilled or len(selected_map[donor]) <= SECTION_MIN_ITEMS:
                continue
            for candidate in selected_map[donor]:
                if not candidate_can_fill_section(candidate, underfilled):
                    continue
                donor_score = get_score_value(candidate, SECTION_SCORE_KEY_MAP[donor]).get("total_score", 0)
                target_score = get_score_value(candidate, SECTION_SCORE_KEY_MAP[underfilled]).get("total_score", 0)
                llm_score = get_llm_importance_score(candidate)
                move_rank = (donor_score - target_score, -target_score, -llm_score, donor)
                identity = candidate_identity(candidate)
                if best_move is None or move_rank < best_move[0]:
                    best_move = (move_rank, identity, candidate)

        if not best_move:
            break

        donor_name = best_move[0][3]
        candidate = best_move[2]
        selected_map[donor_name] = [
            item for item in selected_map[donor_name]
            if candidate_identity(item) != candidate_identity(candidate)
        ]
        selected_map[underfilled].append(candidate)

    for section_name in section_order:
        ranked = rank_candidates(selected_map[section_name], SECTION_SCORE_KEY_MAP[section_name])
        selected_map[section_name] = select_section_candidates(ranked, SECTION_SCORE_KEY_MAP[section_name])

    return selected_map


def build_editorial_selections(raw_clusters: Sequence[Dict[str, object]]) -> Dict[str, object]:
    quick_hits_consensus: List[Dict[str, object]] = []
    quick_hits_exclusive: List[Dict[str, object]] = []
    china_candidates: List[Dict[str, object]] = []
    global_candidates: List[Dict[str, object]] = []
    business_candidates: List[Dict[str, object]] = []
    tech_candidates: List[Dict[str, object]] = []
    section_pools: Dict[str, List[Dict[str, object]]] = {
        "china": [],
        "global": [],
        "business": [],
        "tech": [],
    }

    eligible_clusters = [
        cluster
        for cluster in raw_clusters
        if cluster.get("llm_importance_score", {}).get("eligible", True)
    ]

    quick_hits_consensus = build_quick_hits_consensus_candidates(eligible_clusters)
    unique_clusters = dedupe_similar_candidates(eligible_clusters, "quick_hits_score")

    for cluster in unique_clusters:
        if cluster.get("major_exclusive_score", {}).get("eligible") and not any(
            candidates_match(cluster, consensus_candidate) for consensus_candidate in quick_hits_consensus
        ):
            quick_hits_exclusive.append(cluster)

        section_score = cluster.get("section_scores", {})
        section_candidates: List[Tuple[str, Dict[str, object]]] = []
        for name in ("china", "global", "business", "tech"):
            score = section_score.get(name, {})
            if not score:
                continue
            if name == "china" and cluster.get("china_focus_score", {}).get("background_only"):
                continue
            if score.get("section_fit", 0) <= 0:
                continue
            section_pools[name].append(cluster)
            section_candidates.append((name, score))

        if not section_candidates:
            continue

        best_section, _ = max(
            section_candidates,
            key=lambda item: (
                item[1].get("eligible", False),
                item[1].get("total_score", 0),
                1 if item[0] == "china" and cluster.get("china_focus_score", {}).get("eligible") else 0,
            ),
        )
        cluster["assigned_section"] = best_section

        if best_section == "china":
            china_candidates.append(cluster)
        elif best_section == "global":
            global_candidates.append(cluster)
        elif best_section == "business":
            business_candidates.append(cluster)
        else:
            tech_candidates.append(cluster)

    ranked_quick_hits_consensus = select_ranked_candidates(
        quick_hits_consensus,
        "quick_hits_score",
        min_items=0,
        max_items=QUICK_HITS_MAX_TOTAL,
    )
    ranked_quick_hits_exclusive = select_ranked_candidates(
        quick_hits_exclusive,
        "major_exclusive_score",
        min_items=0,
        max_items=QUICK_HITS_MAX_EXCLUSIVE,
    )

    regular_cap = max(0, QUICK_HITS_MAX_TOTAL - len(ranked_quick_hits_exclusive))
    ranked_quick_hits_consensus = ranked_quick_hits_consensus[:regular_cap]
    quick_hits_total = ranked_quick_hits_consensus + ranked_quick_hits_exclusive
    quick_hits_total = quick_hits_total[:QUICK_HITS_MAX_TOTAL]

    if not quick_hits_total and eligible_clusters:
        fallback_quick_hits = rank_candidates(eligible_clusters, "quick_hits_score")[: min(SECTION_MIN_ITEMS, len(eligible_clusters))]
        quick_hits_total = [{**candidate, "quick_hits_mode": "fallback"} for candidate in fallback_quick_hits]
    else:
        ranked_quick_hits_consensus = [{**candidate, "quick_hits_mode": "regular"} for candidate in ranked_quick_hits_consensus]
        ranked_quick_hits_exclusive = [{**candidate, "quick_hits_mode": "exclusive"} for candidate in ranked_quick_hits_exclusive]
        quick_hits_total = (ranked_quick_hits_consensus + ranked_quick_hits_exclusive)[:QUICK_HITS_MAX_TOTAL]

    selected_map = rebalance_section_candidates(
        {
            "china": select_section_candidates(china_candidates, "section_scores.china"),
            "global": select_section_candidates(global_candidates, "section_scores.global"),
            "business": select_section_candidates(business_candidates, "section_scores.business"),
            "tech": select_section_candidates(tech_candidates, "section_scores.tech"),
        },
        section_pools,
    )
    for section_name in ("business", "tech"):
        if selected_map[section_name]:
            continue
        already_selected = [
            candidate
            for candidates in selected_map.values()
            for candidate in candidates
        ]
        fallbacks = section_fallback_candidates(raw_clusters, section_name, already_selected)
        if fallbacks:
            section_pools[section_name].extend(fallbacks)
            selected_map[section_name] = select_section_candidates(
                fallbacks,
                SECTION_SCORE_KEY_MAP[section_name],
            )

    return {
        "quick_hits_consensus": ranked_quick_hits_consensus[:QUICK_HITS_MAX_TOTAL],
        "quick_hits_exclusive_candidates": ranked_quick_hits_exclusive[:QUICK_HITS_MAX_EXCLUSIVE],
        "quick_hits_candidates": quick_hits_total,
        "china_focus_candidates": selected_map["china"],
        "global_affairs_candidates": selected_map["global"],
        "business_market_candidates": selected_map["business"],
        "tech_ai_candidates": selected_map["tech"],
        "section_candidate_pools": {
            SECTION_SELECTION_KEY_MAP[name]: list(section_pools[name])
            for name in section_pools
        },
        "section_pool_counts": {
            SECTION_SELECTION_KEY_MAP[name]: len(section_pools[name])
            for name in section_pools
        },
    }


def score_quick_hits_consensus(cluster: List[NewsItem]) -> Dict[str, object]:
    sorted_cluster = sorted(
        cluster,
        key=lambda item: item.published or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    lower_corpus = build_cluster_corpus(sorted_cluster).lower()
    source_count = len({canonicalize_source_name(item.source) for item in sorted_cluster})
    tier1_count = count_tier1_sources(sorted_cluster)
    issue_score = max(
        weighted_pattern_score(lower_corpus, GLOBAL_AFFAIRS_PATTERNS),
        weighted_pattern_score(lower_corpus, BUSINESS_MARKETS_PATTERNS),
        weighted_pattern_score(lower_corpus, TECH_AI_PATTERNS),
        score_china_focus(sorted_cluster)["total_score"],
    )
    total_score = min(6, source_count * 2) + min(3, tier1_count) + recency_score(sorted_cluster[0].published) + min(4, issue_score)
    return {
        "source_count_bonus": min(6, source_count * 2),
        "tier1_bonus": min(3, tier1_count),
        "freshness_bonus": recency_score(sorted_cluster[0].published),
        "issue_score": min(4, issue_score),
        "total_score": total_score,
        "eligible": source_count >= 2,
    }


def summarize_cluster_key(clusters: Sequence[Dict[str, object]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for cluster in clusters:
        value = clean_text(cluster.get(key, ""))
        if value:
            counts[value] += 1
    return {
        name: count
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    }


def select_cluster_items_with_source_diversity(
    sorted_cluster: Sequence[NewsItem],
    limit: int,
) -> List[NewsItem]:
    if limit <= 0:
        return []

    selected: List[NewsItem] = []
    seen_keys = set()
    seen_sources = set()

    for item in sorted_cluster:
        source = canonicalize_source_name(item.source)
        item_key = clean_text(item.link) or clean_text(item.title)
        if not item_key or item_key in seen_keys or source in seen_sources:
            continue
        selected.append(item)
        seen_keys.add(item_key)
        seen_sources.add(source)
        if len(selected) >= limit:
            return selected

    for item in sorted_cluster:
        item_key = clean_text(item.link) or clean_text(item.title)
        if not item_key or item_key in seen_keys:
            continue
        selected.append(item)
        seen_keys.add(item_key)
        if len(selected) >= limit:
            break

    return selected


def serialize_cluster(
    cluster: List[NewsItem],
    max_items: Optional[int] = None,
    summary_limit: Optional[int] = None,
) -> Dict[str, object]:
    sorted_cluster = sorted(
        cluster,
        key=lambda item: item.published or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    serialized_items = select_cluster_items_with_source_diversity(
        sorted_cluster,
        max_items or len(sorted_cluster),
    )
    unique_sources = list(dict.fromkeys(canonicalize_source_name(item.source) for item in sorted_cluster))
    unique_feeds = list(dict.fromkeys(feed for item in sorted_cluster for feed in item.feed_names))
    section_scores = {
        "china": score_section_candidate(sorted_cluster, "china"),
        "global": score_section_candidate(sorted_cluster, "global"),
        "business": score_section_candidate(sorted_cluster, "business"),
        "tech": score_section_candidate(sorted_cluster, "tech"),
    }
    serialized = {
        "headline": sorted_cluster[0].title,
        "primary_source": canonicalize_source_name(sorted_cluster[0].source),
        "primary_feed": sorted_cluster[0].feed_names[0] if sorted_cluster[0].feed_names else canonicalize_source_name(sorted_cluster[0].source),
        "source_count": len(set(unique_sources)),
        "sources": unique_sources,
        "feeds": unique_feeds,
        "items": [
            {
                "source": canonicalize_source_name(item.source),
                "title": item.title,
                "summary": (item.summary[:summary_limit] if summary_limit else item.summary),
                "link": item.link,
                "published": item.published.isoformat() if item.published else None,
                "feed_names": item.feed_names,
            }
            for item in serialized_items
        ],
        "quick_hits_score": score_quick_hits_consensus(sorted_cluster),
        "section_scores": section_scores,
    }
    if serialized["source_count"] == 1:
        serialized["major_exclusive_score"] = score_major_exclusive(sorted_cluster)
    serialized["china_focus_score"] = section_scores["china"]
    return serialized


def compact_candidate_for_prompt(candidate: Dict[str, object], max_items: int = 2) -> Dict[str, object]:
    compact_items = []
    for item in candidate.get("items", [])[:max_items]:
        compact_items.append(
            {
                "source": canonicalize_source_name(str(item.get("source", ""))),
                "title": clean_text(str(item.get("title", ""))),
                "summary": condense_summary_sentence(str(item.get("summary", "")), limit=110),
                "link": clean_text(str(item.get("link", ""))),
            }
        )

    compact: Dict[str, object] = {
        "headline": clean_text(candidate.get("headline", "")),
        "sources": list(candidate.get("sources", []))[:4],
        "source_count": candidate.get("source_count", 0),
        "llm_importance_score": candidate.get("llm_importance_score", {}),
        "quick_hits_score": candidate.get("quick_hits_score", {}),
        "section_scores": candidate.get("section_scores", {}),
        "china_focus_score": candidate.get("china_focus_score", {}),
        "items": compact_items,
    }
    if candidate.get("major_exclusive_score"):
        compact["major_exclusive_score"] = candidate.get("major_exclusive_score", {})
    return compact


def build_llm_score_batch_payload(clusters: Sequence[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    payload: Dict[str, Dict[str, object]] = {}
    for idx, cluster in enumerate(clusters, start=1):
        key = f"n{idx}"
        top_items = []
        for item in cluster.get("items", [])[:2]:
            top_items.append(
                {
                    "source": canonicalize_source_name(str(item.get("source", ""))),
                    "title": clean_text(item.get("title", "")),
                    "summary": condense_summary_sentence(item.get("summary", ""), limit=140),
                }
            )
        payload[key] = {
            "headline": clean_text(cluster.get("headline", "")),
            "sources": cluster.get("sources", [])[:4],
            "source_count": cluster.get("source_count", 0),
            "primary_source": canonicalize_source_name(str(cluster.get("primary_source", ""))),
            "top_items": top_items,
        }
    return payload


def request_llm_importance_scores(batch: Sequence[Dict[str, object]]) -> Dict[str, int]:
    if not batch:
        return {}

    payload = build_llm_score_batch_payload(batch)
    prompt = f"""
你是新闻编辑部的“重要性打分器”。请只根据每条新闻的公共重要性，为其打 0-10 的整数分。

评分规则：
- 0 = 不重要
- 5 = 普通新闻
- 10 = 非常重要

硬规则：
1. 只输出 JSON 对象，键必须保持原样。
2. 只能输出 0-10 的整数。
3. 重点看：全球公共影响、政策/战争/市场/技术范式、跨境外溢性、动作确定性、主流信源可靠度。
4. 地方性事故、普通公司动态、娱乐/体育、软新闻，除非存在显著系统性影响，否则一般不应高于 4 分。
5. 评分要克制，不要普遍打高分。

待评分新闻：
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
    result = extract_json_object(call_llm(prompt))
    if not result:
        raise RuntimeError("LLM 重要性评分返回为空。")

    scores: Dict[str, int] = {}
    for key in payload:
        raw_value = result.get(key)
        if raw_value is None:
            raise RuntimeError(f"LLM 重要性评分缺少键: {key}")
        try:
            score = int(raw_value)
        except (TypeError, ValueError):
            raise RuntimeError(f"LLM 重要性评分不是整数: {key}={raw_value}")
        scores[key] = max(0, min(10, score))
    return scores


def attach_llm_importance_scores(raw_clusters: Sequence[Dict[str, object]]) -> None:
    batch_size = get_llm_score_batch_size()
    for start in range(0, len(raw_clusters), batch_size):
        batch = list(raw_clusters[start:start + batch_size])
        score_map = request_llm_importance_scores(batch)
        for offset, cluster in enumerate(batch, start=1):
            key = f"n{offset}"
            score = score_map[key]
            cluster["llm_importance_score"] = {
                "score": score,
                "threshold": LLM_IMPORTANCE_THRESHOLD,
                "eligible": score > LLM_IMPORTANCE_THRESHOLD,
            }

# =============================================================================
# § 5  终极 Prompt 逻辑 (投行级严厉指令)
# =============================================================================

def build_prompt(clusters, history):
    today_str = get_local_now().strftime("%Y-%m-%d")
    if clusters and isinstance(clusters[0], dict):
        raw_clusters = list(clusters[: get_prompt_cluster_limit()])
    else:
        raw_clusters = [
            serialize_cluster(
                cluster,
                max_items=get_prompt_items_per_cluster(),
                summary_limit=get_prompt_summary_limit(),
            )
            for cluster in clusters[: get_prompt_cluster_limit()]
        ]
    selections = build_editorial_selections(raw_clusters)
    china_background_only_rejects = [
        {
            "headline": cluster["headline"],
            "sources": cluster["sources"],
            "china_focus_score": cluster.get("china_focus_score", {}),
        }
        for cluster in raw_clusters
        if cluster.get("china_focus_score", {}).get("background_only")
    ]
    source_inventory = {
        "feed_roster": list(DEFAULT_SOURCES.keys()),
        "primary_source_counts": dict(list(summarize_cluster_key(raw_clusters, "primary_source").items())[:12]),
        "primary_feed_counts": dict(list(summarize_cluster_key(raw_clusters, "primary_feed").items())[:12]),
    }
    compact_mode = should_avoid_heavy_llm_repairs()

    payload = {
        "quick_hits_candidates": [compact_candidate_for_prompt(item) for item in selections["quick_hits_candidates"]],
        "quick_hits_consensus": [compact_candidate_for_prompt(item) for item in selections["quick_hits_consensus"]],
        "quick_hits_exclusive_candidates": [compact_candidate_for_prompt(item) for item in selections["quick_hits_exclusive_candidates"]],
        "china_focus_candidates": [compact_candidate_for_prompt(item) for item in selections["china_focus_candidates"]],
        "global_affairs_candidates": [compact_candidate_for_prompt(item) for item in selections["global_affairs_candidates"]],
        "business_market_candidates": [compact_candidate_for_prompt(item) for item in selections["business_market_candidates"]],
        "tech_ai_candidates": [compact_candidate_for_prompt(item) for item in selections["tech_ai_candidates"]],
        "china_background_only_rejects": china_background_only_rejects[:12],
        "source_inventory": source_inventory,
    }
    if not compact_mode:
        payload["raw_clusters"] = raw_clusters

    if compact_mode:
        headline_briefing = [
            {
                "headline": clean_text(candidate.get("headline", "")),
                "sources": candidate.get("sources", [])[:3],
                "llm_importance_score": candidate.get("llm_importance_score", {}).get("score", 0),
            }
            for candidate in rank_candidates(raw_clusters, "quick_hits_score")[:8]
        ]
        compact_prompt = f"""
你是严谨的中文新闻简报编辑。今天是现实世界中的 {today_str}。

你只需要完成 2 件事：
1. 根据【今日历史数据】写 1 段“历史上的今天”开场。
2. 根据【重点候选新闻】写 1 行中文 Subject。

输出要求：
1. 只输出 Markdown，不要解释，不要代码块。
2. 必须严格输出以下骨架，顺序不能变：
历史上的今天（[年份]）：[100字左右中文客观描述]。 —— [来源: Wikipedia](URL)
---
## 【Quick Hits】

## 🇨🇳【中国与世界 / China & The World】

## 🌍【全球局势 / Global Affairs】

## 📈【商业与市场 / Business & Markets】

## 🚀【科技与AI / Tech & AI】
---
Subject: [中文短句；中文短句]
3. 不要给四个新闻板块填写任何正文，保持为空，系统会后续填充。
4. 全文必须为简体中文；只有 Subject 行保留 `Subject:` 英文前缀。
5. Subject 必须用中文提炼最重要的 1-2 个事件短句，用全角分号连接；系统会自动补上固定前缀【The Babel Brief】。

【今日历史数据（Wikipedia）】：
{history or '无可用数据'}

【重点候选新闻】：
{json.dumps(headline_briefing, ensure_ascii=False, indent=2)}
"""
        return compact_prompt

    rules = f"""
你是一个极其严谨的顶级投行首席情报分析师与资深新闻主编。
【系统最高指令】：严禁输出客套话。今天是现实世界中的 {today_str}。你在执行“音乐史上的今天”时，必须严格比对这个日期。

请基于以下提供的【原始新闻数据】，严格按照给定的结构和绝对红线生成专业简报。

【绝对客观红线（贯穿全文，极度严厉）】：
你必须恪守顶级投行与通讯社的新闻专业主义。绝对禁止使用“狂揽”“暴利”“震惊”“惨遭”“史诗级”等带有情绪诱导、主观评判或夸张修辞的词汇。所有标题和总结必须 100% 忠于原文，只能使用克制、中立的陈述句。

【全板块统一评分红线】：
- 每条候选新闻都带有 `llm_importance_score.score`，它来自 Gemini 重要性评分器。
- 只允许选择 `llm_importance_score.score > 4` 的新闻。
- Quick Hits 与所有深读板块都必须按 `llm_importance_score.score` 从高到低排序；如果分数相同，再参考原有候选顺序。

--- 简报生成结构与物理规则 ---

1. 破冰开场（巴别塔的记忆）：
   - 从【今日历史数据】中挑选 1 条最具“知识锚点”属性的真实事件。
   - 不设领域限制，可以是文化里程碑、科学奇迹、历史冷知识或改变世界轨迹的重大节点。
   - 拒绝平庸流水账，只保留真正能拓宽读者认知边界的事件。
   - 排版必须严格如下：
     历史上的今天（[年份]）：[客观陈述该事件的重要性或奇妙之处，约 100 字]。 —— [来源: Wikipedia](提供的URL)
   - 然后换行输出 `---`。

2. 顶部速递（必须输出大标题 `## 【Quick Hits】`）：
   - 【共识轨】：必须且只能挑选被 2 家及以上不同媒体共同报道的事件作为常规新闻。
   - 【独家轨】：Quick Hits 不是纯双源门槛，允许“重大独家”破格入选；但单源新闻只有在通过下方评分机制后，才允许进入 Quick Hits，并且最多 1 到 3 条。
   - 【重大独家评分机制（10 分制，硬门槛）】：
     1. Source Authority / 信源权威（0-2 分）：Reuters/AP/AFP/Bloomberg/FT/WSJ 才能拿满 2 分；非 Tier-1 信源直接视为 0 分。
     2. Systemic Impact / 系统性影响（0-4 分）：必须是战争升级、重大外交/制裁/关税、央行利率决策、国家级安全行动、金融系统风险、全球供应链断裂等事件，才可得高分。
     3. Spillover / 外溢性（0-2 分）：是否直接影响全球市场、能源、航运、芯片、AI 基础设施、汇率、通胀或大国博弈。
     4. Irreversibility / 动作确定性（0-2 分）：是否已经发生明确动作，如“出兵、制裁、生效、营救、暂停、禁运、加息、违约、破产、批准、打击”。
   - 【入选阈值】：
     - 总分必须 >= 8 分；
     - Systemic Impact 必须单项达到 4 分；
     - 信源权威必须为 Tier-1；
     - 只有 JSON 里的 `quick_hits_exclusive_candidates` 才允许作为 Quick Hits 独家候选，其他所有单源新闻一律禁止进入 Quick Hits。
   - 【一票否决项】：融资、产品发布、合作、采访、评论、播客、普通观点、常规创业新闻、普通公司动态，哪怕来自知名媒体，也绝对禁止以独家重磅身份进入 Quick Hits。
   - 【强制排序】：先输出所有共识新闻，再把带有 `[🚨独家重磅]` 标签的新闻统一放到列表最底部。
   - 【数量规则】：【无下限，只设上限】。除了得分足够高的重大独家外，只要满足双源共识，就有多少写多少；如果当天只有 3 条满足双源，就只输出 3 条。绝对禁止拉低门槛，用普通单源新闻凑数。Quick Hits 总数绝对不超过 12 条，其中重大独家默认最多 2 条。
   - 【长度红线】：每条 Quick Hits 只能用一句话，控制在紧凑长度内，只写核心事实，不要背景解释，不要补充分析。
   - 【Emoji 红线】：常规多源报道每条前面只能有 1 个最贴切的 emoji，禁止用国旗批量填充。仅允许使用 `🌍 / 📈 / 🤖 / ⚡ / 🩺 / 🏛️ / 📰` 之一；重大独家固定以 `🚨 [独家重磅]` 开头，前面禁止再加任何其他 emoji。
   - 【程序化数据边界】：优先使用 JSON 中已按重要级排序的 `quick_hits_candidates`、`quick_hits_consensus` 与 `quick_hits_exclusive_candidates`；不要从候选池之外另行发明 Quick Hits。
   - 排版必须严格使用无序列表 `*`：
     * （常规多源报道）* [合适 emoji] **[简短中立标题]**：一句话概括事实。[[来源: 媒体A](URL), [来源: 媒体B](URL)]
     * （极少数单源破格）* 🚨 `[独家重磅]` **[简短中立标题]**：一句话概括事实。[[来源: 媒体](URL)]

3. 涉华聚焦（必须输出大标题 `## 🇨🇳【中国与世界 / China & The World】`）：
   - 【绝对主角红线（最高拦截级别）】：新闻的核心动作发起方或核心动作直接承受方必须是中国实体，例如中国政府出台政策、中企重大出海动作、外国对华直接制裁。
   - 🚫 【防背景音穿透（物理隔离）】：绝对禁止把中国仅作为背景板、分析因素或竞争环境的新闻归入此列。若主角是外国企业、外国政府或第三方机构，而中国只是其商业决策背景，该新闻必须退回【商业与市场】或【全球局势】。
   - 【显性判定红线】：本板块只收录原新闻中直接、显性涉及中国政策、中美博弈、对华关税、两岸三地、中企出海、对华制裁、对华投资限制等事件。严禁“二次脑补”。
   - 【程序化入选机制】：
     1. China Entity / 中国实体分（0-3 分）：标题直接点名 China/Beijing/Chinese/Hong Kong/Taiwan 或中国公司，才可得高分。
     2. China Role / 主角角色分（0-3 分）：中国实体必须是动作发起方或直接承受方；若只是背景环境，则不能得分。
     3. Policy & Strategic Relevance / 政策战略分（0-2 分）：涉及关税、制裁、政策、外交、军演、芯片、稀土、中企出海等，才可得分。
     4. Background Penalty / 背景板惩罚（-4 分）：若命中“因为中国竞争”“中国市场疲弱”“受中国需求影响”等背景板模式，直接重罚。
   - 【入选阈值】：
     - 总分必须 >= 5 分；
     - 中国实体分必须 >= 2；
     - 主角角色分必须 >= 2；
     - 命中背景板惩罚则直接禁止入选。
   - 【强制数据边界】：
     - 只有 JSON 里的 `china_focus_candidates` 才允许进入 `## 🇨🇳【中国与世界 / China & The World】`。
     - JSON 里的 `china_background_only_rejects` 是明确的误归类黑名单，绝对禁止放进涉华板块。
   - 【防重叠红线】：被归入本板块的新闻，绝对禁止在后续全球局势、商业与市场、科技与 AI 板块重复出现。
   - 【数量规则】：默认最少 3 条、最多 6 条；只有当候选中出现显著高分且具有连续重大性时，才允许突破至 8 条。
   - 【打分择优】：严格优先使用 JSON 中按分数排序的 `china_focus_candidates`，按重要级由高到低择优入选；若严格入选不足 3 条，才允许在同一候选池中向下放宽阈值补足到 3 条。
   - 【独立序号与排版】：本板块内序号必须从 1 开始。

4. 全球核心矩阵深读：
   - 必须输出以下固定标题：
     `## 🌍【全球局势 / Global Affairs】`
     `## 📈【商业与市场 / Business & Markets】`
     `## 🚀【科技与AI / Tech & AI】`
   - 全球局势：大国冲突、宏观政策、选举、外交。
   - 商业与市场：宏观经济数据、货币政策、资本市场波动、跨国巨头重大商业行为。
   - 科技与 AI：仅限头部 AI 公司模型演进、底层算力与基础设施、以及 Stratechery/Tangle 这类符合宏观与商业重要性的深度观点。
   - 严禁纳入消费电子评测、小众开源琐事、DIY 项目、论坛软文、军工装备和纯学术基础科学。
   - 每个板块内部序号都必须独立从 1 开始。
   - 若为单源独家报道，标题末尾必须标注 `(独家报道)`；若为深度观点，标题末尾必须标注 `(深度观点)`。
   - 【默认数量规则】：`全球局势`、`商业与市场`、`科技与AI` 每个板块默认最少 3 条、最多 6 条；只有当高分候选明显超出 6 条且连续具备重大性时，才允许突破到 8 条。
   - 【程序化打分机制】：你必须优先使用 JSON 中已经完成打分和初筛的 `global_affairs_candidates`、`business_market_candidates`、`tech_ai_candidates`，按分数高低择优选稿；若严格筛选后不足 3 条，只能在对应候选池内部向下放宽阈值补足，不得跨板块乱借、不准从无关原始新闻凑数。
   - 【信源比例调控】：参考 JSON 里的 `source_inventory`。同一 primary source 默认在单个板块内最多出现 2 条；`Techmeme` 作为聚合器默认在单个板块内最多出现 1 条，且只应在其对应原始发布者无法明确识别时才保留为来源展示。

5. 深度新闻全天候统一排版骨架（适用于所有非 Quick Hits 内容）：
   - 每条深度新闻都必须以 `### [当前板块独立序号]. [事件标题]` 开头。
   - 绝对禁止把整条深度新闻写成有序列表容器，例如 `1. 标题 / 2. 全景综述 / 3. 核心事实` 这种格式一律禁止。
   - 标题下方的所有字段都必须使用单层无序列表 `- `，不得把字段编号化。
   - `溯源印证` 下方必须使用 `* **媒体名**: [外媒原报道真实标题](URL)`；绝对禁止拆成“标题”“链接”两行。
   - `报道概括 / 两方观点 / AI推演` 可以综合多家媒体、机构声明、数据源和社交媒体公开信息（包括 X），但每一处观点或判断必须能回到明确出处 URL；严禁无链接来源、严禁编造 URL。
   - `AI推演` 必须模仿 Tangle News 的 “My take / Tangle's take” 写法：先承认争议双方最强论点，再给出编辑部自己的判断。必须写成单个 bullet 行，禁止标签化对仗结构，禁止再嵌套子列表。
   ### [当前板块独立序号]. [事件标题]
   - 📰 **全景综述**：直接概括原始报道本身，写清谁做了什么、发生了什么、报道给出的关键背景和直接后果。控制在 80-130 字。禁止写“这条新闻的重要性在于”“关键变量是”等点评分析句式；句末必须以可点击链接标注主要出处。
   - ⏱️ **新闻时间线（条件触发）**：仅当事件本身跨越多个明确时间节点时触发；严禁把不同媒体的发布时间拼成时间线。
   - 📌 **核心事实**：只写“发生了什么”——即具体动作、关键数字、当事方、时间节点，纯事实罗列，每条事实句末括注信息来源媒体名。禁止出现任何背景介绍、意义阐释或分析性语言；如果一句话在“全景综述”里已经出现过相似意思，则必须删掉。
   - 🔴 **[一方观点 / What one side is saying]**：模仿 Tangle 的观点综述，先用一句话概括这一方的核心看法，再解释其理由。必须写出具名阵营、利益相关方、政策派别或市场群体；禁止写成媒体视角复述标题；句末必须以可点击链接标注该观点出处。
   - 🔵 **[另一方观点 / What the other side is saying]**：模仿 Tangle 的观点综述，概括另一方、反对方、受影响方或市场反馈的最强论点。若不是左右派议题，也要找出真实利益冲突或政策取舍；禁止空泛写“外部反馈仍需观察”；句末必须以可点击链接标注该观点出处。
   - ⚖️ **AI推演**：模仿 Tangle 的 “My take”：先公平承认两方各自说对了什么，再给出清晰编辑判断。60-100 字，必须落到一个明确结论，禁止“优势/风险”“一方面/另一方面”等对仗模板；句末必须以可点击链接标注判断所依据的来源。
   - 🔗 **溯源印证**：必须列出外媒原生标题和链接。

6. 结尾彩蛋：
   - 必须用 `---` 分割，并输出标题 `🎵 **今日回响**`。
   - 【时间锚点（最高红线）】：所选专辑的官方首发日期，月份和日期必须与今天的现实日期完全一致。宁可冷门，绝不允许错日期。
   - 【质量红线】：推荐理由必须像一篇专业乐评，不是泛泛而谈的“好听”“经典”。优先参考 Apple Music 编辑推荐语、Pitchfork、Rolling Stone、Album of the Year、AllMusic 等优质乐评或可验证音乐事实，但正文不必强行点名任何媒体、评分或作者。
   - 【硬性事实门槛】：仅写“AllMusic 流派/Styles 标签”“Wikipedia 录音室专辑”“Wikidata 厂牌/发行信息”一律不合格；这些只能作为辅助材料，不能替代真实评论论点、录音室技术、制作班底、历史评分地位、奖项、榜单表现或对声音结构的专业判断。
   - 【禁止事项】：禁止编造发行日期、禁止捏造媒体评分、禁止空洞抒情、禁止用万能褒义词堆砌。
   - 排版与语气必须严格如下：
     ### 《[专辑名]》
     > 🎤 **[歌手/乐队]** · 📅 [首发精确日期] · 💽 [流派]
     >
     > **📝 编辑推荐 / Editor's Notes：**
     > [撰写 100-150 字的专业乐评。可参考 Apple Music 编辑推荐语和优质音乐媒体的评论论点，但要转译成流利中文段落，不要机械点名来源。绝对拒绝浮夸辞藻，尊重客观音乐史。绝不允许使用空洞修辞，绝不允许凭空捏造主观听感。]

7. 邮件标题元数据：
   - 全文最后必须另起一行，仅输出以下纯文本格式：
     Subject: [用中文提炼当天最重要的 1-2 个事件短句，用全角分号“；”连接]
   - 只输出事实短句，不要日期，不要解释，不要加引号，不要加英文前缀。
   - 系统会自动补上固定前缀【The Babel Brief】。
   - 示例：Subject: 美军成功营救驻伊坠机飞行员；Planet Labs对伊实施图像静默
"""
    return f"{rules}\n\n【今日历史数据（Wikipedia）】：\n{history or '无可用数据'}\n\n【今日新闻 JSON 数据】：\n{json.dumps(payload, ensure_ascii=False, indent=2)}"

# =============================================================================
# § 6  输出、LLM 与发信能力
# =============================================================================

EMAIL_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{page_title}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Doto:wght@400;700&family=Space+Grotesk:wght@300;400;500;700&family=Space+Mono:wght@400;700&display=swap');
:root {{
    color-scheme: light;
    --black: #F5F5F5;
    --surface: #FFFFFF;
    --surface-raised: #F0F0F0;
    --border: #E8E8E8;
    --border-visible: #CCCCCC;
    --text-disabled: #999999;
    --text-secondary: #666666;
    --text-primary: #1A1A1A;
    --text-display: #000000;
    --accent: #D71921;
    --interactive: var(--text-display);
    --display-md: 2.25rem;
    --heading: 1.5rem;
    --subheading: 1.125rem;
    --body: 1rem;
    --caption: 0.75rem;
    --space-sm: 8px;
    --space-md: 16px;
    --space-lg: 24px;
    --space-xl: 32px;
    --space-2xl: 48px;
    --space-3xl: 64px;
    --space-4xl: 96px;
}}
* {{
    box-sizing: border-box;
    letter-spacing: 0;
}}
body {{
    margin: 0;
    padding: var(--space-3xl) var(--space-lg);
    background: var(--black);
    color: var(--text-primary);
    font-family: "Space Grotesk", "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: var(--body);
    line-height: 1.5;
}}
.email-container {{
    max-width: 760px;
    margin: 0 auto;
    padding: 0;
    background: transparent;
    border: 0;
    border-radius: 0;
    box-shadow: none;
}}
.hero {{
    margin: 0 0 var(--space-3xl);
}}
.hero img {{
    width: 100%;
    display: block;
    border-radius: 0;
}}
h1 {{
    margin: 0 0 var(--space-2xl);
    padding: 0 0 var(--space-lg);
    border-bottom: 1px solid var(--border-visible);
    color: var(--text-display);
    font-size: var(--display-md);
    line-height: 1.1;
    font-weight: 700;
}}
h2 {{
    margin: var(--space-3xl) 0 var(--space-lg);
    padding: var(--space-md) 0 0;
    border-top: 1px solid var(--border-visible);
    border-left: 0;
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.2;
    font-weight: 700;
    text-transform: uppercase;
}}
h3 {{
    margin: var(--space-xl) 0 var(--space-md);
    color: var(--text-display);
    font-size: var(--subheading);
    line-height: 1.3;
    font-weight: 700;
}}
p {{
    margin: 0 0 var(--space-md);
    color: var(--text-primary);
    font-size: var(--body);
    line-height: 1.6;
}}
.history-note {{
    margin: 0 0 var(--space-xl);
    color: var(--text-primary);
    font-size: var(--body);
    line-height: 1.5;
}}
.history-label {{
    color: var(--text-display);
    font-weight: 700;
}}
ul {{
    margin: 0 0 var(--space-xl);
    padding: 0;
    list-style: none;
    border-top: 1px solid var(--border);
}}
li {{
    margin: 0;
    padding: 12px 0;
    border-bottom: 1px solid var(--border);
    color: var(--text-primary);
    font-size: var(--body);
    line-height: 1.6;
}}
ul ul {{
    margin-top: var(--space-sm);
    margin-bottom: 0;
    padding-left: var(--space-lg);
    border-top: 1px solid var(--border);
}}
blockquote {{
    margin: var(--space-lg) 0;
    padding: var(--space-md);
    background: transparent;
    border: 1px solid var(--border-visible);
    border-radius: 4px;
    color: var(--text-primary);
}}
a {{
    color: var(--interactive);
    text-decoration: none;
    border-bottom: 1px solid currentColor;
}}
a:hover {{
    color: var(--text-display);
    border-bottom-color: var(--text-display);
}}
strong {{
    color: var(--text-display);
    font-weight: 700;
}}
code {{
    padding: 1px 4px;
    background: var(--surface-raised);
    border: 1px solid var(--border-visible);
    border-radius: 4px;
    color: var(--text-display);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
}}
hr {{
    border: 0;
    border-top: 1px solid var(--border-visible);
    margin: var(--space-3xl) 0;
}}
@media (max-width: 640px) {{
    body {{
        padding: var(--space-2xl) var(--space-md);
    }}
    h1 {{
        font-size: var(--heading);
        margin-bottom: var(--space-xl);
    }}
    h2 {{
        margin-top: var(--space-2xl);
    }}
}}
</style>
</head>
<body>
<div class="email-container">
{hero_block}
{content}
</div>
</body>
</html>
"""


def maybe_enable_socks_proxy() -> None:
    proxy_host = os.getenv("SOCKS_PROXY_HOST", "").strip()
    if not proxy_host:
        return

    try:
        import socket
        import socks
    except ImportError:
        print("[WARN] 检测到 SOCKS 代理配置，但未安装 PySocks，跳过代理设置。")
        return

    proxy_port = int(os.getenv("SOCKS_PROXY_PORT", "7890"))
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, proxy_host, proxy_port)
    socket.socket = socks.socksocket
    print(f"[INFO] SOCKS5 代理已启用: {proxy_host}:{proxy_port}")


def get_api_key() -> str:
    for key_name in ("LLM_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "OPENAI_API_KEY"):
        value = os.getenv(key_name, "").strip()
        if value:
            return value
    raise RuntimeError("缺失 API 密钥。请至少配置 GEMINI_API_KEY 或 LLM_API_KEY。")


def get_model_candidates() -> List[str]:
    defaults = [
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
        "gemini-flash-lite-latest",
        "gemini-2.0-flash",
        "gemini-flash-latest",
        "gemini-2.5-pro",
    ]
    explicit = (
        os.getenv("LLM_MODEL")
        or os.getenv("GOOGLE_MODEL")
        or os.getenv("GEMINI_MODEL")
        or ""
    ).strip()
    if explicit:
        return [explicit]
    ordered: List[str] = []
    for model in (ACTIVE_GEMINI_MODEL, *defaults):
        if model and model not in ordered:
            ordered.append(model)
    return ordered


def call_llm(prompt: str) -> str:
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    openai_base = os.getenv("OPENAI_BASE_URL", "").strip()
    api_key = get_api_key()
    timeout_s = int(os.getenv("HTTP_TIMEOUT_SECONDS", "120"))
    print(f"[INFO] LLM prompt 大小: {len(prompt)} chars")
    cache_key = ""

    if provider == "openai" or openai_base:
        model = (
            os.getenv("LLM_MODEL")
            or os.getenv("OPENAI_MODEL")
            or os.getenv("GEMINI_MODEL")
            or "gpt-4o-mini"
        ).strip()
        cache_key = f"openai::{model}::{prompt}"
        if cache_key in LLM_RESPONSE_CACHE:
            print(f"[INFO] 命中 LLM 缓存: {model}")
            return LLM_RESPONSE_CACHE[cache_key]
        response = call_openai_compatible(prompt, model, api_key, openai_base, timeout_s)
        LLM_RESPONSE_CACHE[cache_key] = response
        return response

    models = get_model_candidates()
    cache_key = f"gemini::{','.join(models)}::{prompt}"
    if cache_key in LLM_RESPONSE_CACHE:
        print(f"[INFO] 命中 LLM 缓存: {models[0] if models else 'gemini'}")
        return LLM_RESPONSE_CACHE[cache_key]
    response = call_gemini_native(prompt, models, api_key, timeout_s)
    LLM_RESPONSE_CACHE[cache_key] = response
    return response


def call_openai_compatible(prompt: str, model: str, api_key: str, base_url: str, timeout_s: int) -> str:
    url = f"{(base_url or 'https://api.openai.com/v1').rstrip('/')}/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.10,
        "top_p": 0.80,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    last_error = None
    for attempt in range(3):
        try:
            print(f"[INFO] 正在请求 OpenAI 兼容模型 {model} ({attempt + 1}/3)...")
            response = http_post(url, json=payload, headers=headers, timeout=timeout_s)
            response.raise_for_status()
            result = response.json()
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            if not content:
                raise RuntimeError(f"模型返回空内容: {result}")
            return content
        except Exception as exc:
            last_error = exc
            print(f"[WARN] OpenAI 兼容接口请求失败: {exc}")
            if hasattr(exc, "response") and exc.response is not None:
                print(f"[DEBUG] 服务器返回: {exc.response.text}")
            if attempt < 2:
                time.sleep(5)
    raise RuntimeError("OpenAI 兼容接口连续重试失败。") from last_error


def call_gemini_native(prompt: str, models: Sequence[str], api_key: str, timeout_s: int) -> str:
    global ACTIVE_GEMINI_MODEL
    last_error = None
    retry_count = max(2, int(os.getenv("GEMINI_MAX_RETRIES", "4")))
    connect_timeout = max(10, int(os.getenv("GEMINI_CONNECT_TIMEOUT_SECONDS", "20")))
    for model in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ],
            "generationConfig": {
                "temperature": 0.10,
                "topP": 0.80,
                "responseMimeType": "text/plain",
            },
        }

        for attempt in range(retry_count):
            try:
                print(f"[INFO] 正在请求 Gemini 模型 {model} ({attempt + 1}/{retry_count})...")
                response = http_post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=(connect_timeout, timeout_s),
                )
                response.raise_for_status()
                result = response.json()
                candidates = result.get("candidates", [])
                if not candidates:
                    raise RuntimeError(f"模型返回空候选: {result}")

                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(part.get("text", "") for part in parts if part.get("text")).strip()
                if not text:
                    raise RuntimeError(f"模型返回空文本: {result}")
                ACTIVE_GEMINI_MODEL = model
                print(f"[INFO] 当前运行锁定 Gemini 模型: {model}")
                return text
            except Exception as exc:
                last_error = exc
                print(f"[WARN] Gemini 请求失败: {exc}")
                if hasattr(exc, "response") and exc.response is not None:
                    print(f"[DEBUG] 服务器返回: {exc.response.text}")
                if attempt < retry_count - 1:
                    error_text = str(exc).lower()
                    response_text = ""
                    if hasattr(exc, "response") and exc.response is not None:
                        response_text = clean_text(exc.response.text).lower()
                    is_high_demand = "503" in error_text or "unavailable" in error_text or "high demand" in response_text
                    is_read_timeout = "read timed out" in error_text or "readtimeout" in error_text
                    if is_high_demand:
                        delay = min(45, 12 + attempt * 12)
                    elif is_read_timeout:
                        delay = min(30, 10 + attempt * 8)
                    else:
                        delay = min(12, 2 + attempt * 2)
                    print(f"[INFO] Gemini 重试前等待 {delay} 秒...")
                    time.sleep(delay)

    raise RuntimeError("Gemini 接口连续重试失败。") from last_error


def render_email_html(md_text: str, cover_image_url: str, page_title: str = "The Babel Brief") -> str:
    html_body = markdown.markdown(md_text, extensions=["tables", "fenced_code", "nl2br"])
    html_body = postprocess_email_html(html_body)
    hero_block = ""
    if cover_image_url:
        hero_block = (
            '<div class="hero">'
            f'<img src="{cover_image_url}" alt="Daily briefing cover">'
            "</div>"
        )
    return EMAIL_TEMPLATE.format(
        page_title=html.escape(page_title or "The Babel Brief", quote=False),
        hero_block=hero_block,
        content=html_body,
    )


def postprocess_email_html(html_body: str) -> str:
    body = re.sub(
        r'<p class="history-note"><span class="history-label">历史上的今天</span>：(&lt;p class=&quot;history-note&quot;&gt;.*?&lt;/p&gt;)</p>',
        lambda match: html.unescape(match.group(1)),
        html_body,
        flags=re.DOTALL,
    )
    body = re.sub(r"([。！？!?])\[。?(<a\s+href=)", r"\1\2", body)
    body = re.sub(r"\[。?(<a\s+href=)", r"。\1", body)
    body = body.replace("</a>]", "</a>")

    lines = body.splitlines()
    rewritten: List[str] = []
    index = 0

    while index < len(lines):
        stripped = lines[index].strip()
        if stripped == "<li>🔗 <strong>溯源印证</strong>：</li>":
            rewritten.append("<li>🔗 <strong>溯源印证</strong>：")
            rewritten.append("<ul>")
            index += 1
            while index < len(lines) and re.match(r"^\s*<li><strong>[^<]+</strong>:", lines[index].strip()):
                rewritten.append(lines[index].strip())
                index += 1
            rewritten.append("</ul>")
            rewritten.append("</li>")
            continue
        rewritten.append(lines[index])
        index += 1

    return "\n".join(rewritten)


def fetch_wikipedia_on_this_day(timeout_s: int) -> str:
    try:
        today = get_local_now()
        month = today.strftime("%m")
        day = today.strftime("%d")
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/events/{month}/{day}"
        response = http_get(url, headers={"User-Agent": APP_USER_AGENT}, timeout=timeout_s)
        response.raise_for_status()
        data = response.json()

        history_lines = []
        for event in data.get("events", [])[:20]:
            year = event.get("year", "Unknown")
            text = clean_text(event.get("text", ""))
            link = ""
            if event.get("pages"):
                link = event["pages"][0].get("content_urls", {}).get("desktop", {}).get("page", "")
            if not link:
                link = f"https://en.wikipedia.org/wiki/{month}_{day}"
            history_lines.append(f"- {year}: {text} [URL: {link}]")
        return "\n".join(history_lines)
    except Exception as exc:
        print(f"[WARN] 获取维基百科历史失败: {exc}")
        return ""


def normalize_subject(subject: str) -> str:
    value = clean_text(subject)
    value = re.sub(r"^subject\s*:\s*", "", value, flags=re.IGNORECASE)
    value = value.removeprefix("【The Babel Brief】").strip()
    value = re.sub(r"^The Babel Brief\s*[\|\-:：]\s*", "", value, flags=re.IGNORECASE).strip()
    value = value.strip(" ;；|")
    return f"【The Babel Brief】{value}" if value else "【The Babel Brief】今日全球要闻"


def build_default_subject(clusters: List[List[NewsItem]]) -> str:
    fragments: List[str] = []
    for cluster in clusters:
        title = clean_text(cluster[0].title)
        title = re.sub(r"\s*[-|–—]\s*(Reuters|Bloomberg|AFP|AP|Associated Press|Financial Times|WSJ)\s*$", "", title, flags=re.IGNORECASE)
        title = title.strip(" .,:;：；")
        if title and title not in fragments:
            fragments.append(title)
        if len(fragments) == 2:
            break

    fallback = "；".join(fragments) if fragments else "今日全球要闻"
    return normalize_subject(fallback)


def extract_report_metadata(md_report: str, fallback_subject: str) -> Tuple[str, str, str]:
    subject = fallback_subject
    clean_lines: List[str] = []

    for raw_line in md_report.splitlines():
        stripped = raw_line.strip()
        lower = stripped.lower()
        if lower.startswith("subject:"):
            value = raw_line.split(":", 1)[1].strip()
            if value:
                subject = normalize_subject(value)
            continue
        if lower.startswith("imageprompt:"):
            continue
        clean_lines.append(raw_line)

    return subject, "", "\n".join(clean_lines).strip()


def parse_iso_date(date_text: str) -> Optional[datetime]:
    value = clean_text(date_text)
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def find_today_echo_start(lines: Sequence[str]) -> Optional[int]:
    for index, line in enumerate(lines):
        if "今日回响" not in line:
            continue
        if index > 0 and lines[index - 1].strip() == "---":
            return index - 1
        return index
    return None


def extract_today_echo_metadata(md_text: str) -> Dict[str, str]:
    lines = md_text.splitlines()
    start = find_today_echo_start(lines)
    if start is None:
        return {}

    section_lines = lines[start:]
    album = ""
    artist = ""
    release_date = ""
    genre = ""

    for raw in section_lines:
        stripped = raw.strip().lstrip(">").strip()
        album_match = re.match(r"^###\s+《(.+?)》\s*$", stripped)
        if album_match:
            album = clean_text(album_match.group(1))
            continue

        meta_match = re.search(
            r"🎤\s*\*\*([^*]+)\*\*\s*·\s*📅\s*(\d{4}-\d{2}-\d{2})(?:\s*·\s*💽\s*(.+))?$",
            stripped,
        )
        if meta_match:
            artist = clean_text(meta_match.group(1))
            release_date = clean_text(meta_match.group(2))
            genre = clean_text(meta_match.group(3) or "")
            break

    return {
        "album": album,
        "artist": artist,
        "release_date": release_date,
        "genre": genre,
    }


def normalize_match_text(text: str) -> str:
    value = clean_text(text).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def match_similarity(left: str, right: str) -> float:
    left_norm = normalize_match_text(left)
    right_norm = normalize_match_text(right)
    if not left_norm or not right_norm:
        return 0.0

    base = SequenceMatcher(None, left_norm, right_norm).ratio()
    left_tokens = set(left_norm.split())
    right_tokens = set(right_norm.split())
    overlap = len(left_tokens & right_tokens) / max(1, min(len(left_tokens), len(right_tokens)))
    return max(base, overlap)


def parse_human_date_to_iso(date_text: str) -> str:
    value = clean_text(date_text)
    if not value:
        return ""
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def unique_preserving_order(values: Sequence[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def build_today_echo_evidence(
    source: str,
    album: str,
    artist: str,
    release_dates: Sequence[str],
    url: str = "",
    genres: Optional[Sequence[str]] = None,
    styles: Optional[Sequence[str]] = None,
    summary: str = "",
) -> Optional[Dict[str, object]]:
    exact_dates = unique_preserving_order(
        dt.strftime("%Y-%m-%d")
        for dt in (parse_iso_date(value) for value in release_dates)
        if dt
    )
    if not exact_dates:
        return None
    return {
        "source": source,
        "album": clean_text(album),
        "artist": clean_text(artist),
        "release_dates": exact_dates,
        "url": clean_text(url),
        "genres": unique_preserving_order(genres or []),
        "styles": unique_preserving_order(styles or []),
        "summary": clean_text(summary),
    }


def fetch_musicbrainz_release_group(album: str, artist: str, timeout_s: int) -> Optional[Dict[str, object]]:
    title = clean_text(album)
    performer = clean_text(artist)
    if not title or not performer:
        return None

    try:
        response = http_get(
            "https://musicbrainz.org/ws/2/release-group/",
            params={
                "query": f'releasegroup:"{title}" AND artist:"{performer}"',
                "fmt": "json",
                "limit": 5,
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] MusicBrainz 查询失败: {exc}")
        return None

    best_match: Optional[Dict[str, object]] = None
    best_score = 0.0
    for group in response.json().get("release-groups", []):
        group_title = clean_text(group.get("title", ""))
        group_artist = clean_text(" ".join(part.get("name", "") for part in group.get("artist-credit", [])))
        if clean_text(group.get("primary-type", "")).lower() not in {"album", ""}:
            continue

        title_score = match_similarity(title, group_title)
        artist_score = match_similarity(performer, group_artist) if group_artist else 0.0
        total_score = title_score + artist_score * 0.6
        if total_score <= best_score:
            continue

        best_score = total_score
        best_match = build_today_echo_evidence(
            "MusicBrainz",
            group_title or title,
            group_artist or performer,
            [clean_text(group.get("first-release-date", ""))],
            f"https://musicbrainz.org/release-group/{clean_text(group.get('id', ''))}",
        )

    return best_match if best_match and best_score >= 0.9 else None


def html_fragment_to_text(fragment: str) -> str:
    value = re.sub(r"<script\b.*?</script>", " ", fragment, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"<style\b.*?</style>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"<sup\b.*?</sup>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"<[^>]+>", " ", value)
    return clean_text(html.unescape(value))


def parse_allmusic_rating(page: str) -> str:
    match = re.search(r"\ballmusicRating\s+ratingAllmusic(\d+)\b", page)
    if not match:
        return ""
    raw_score = int(match.group(1))
    return f"{raw_score / 2:g}/5"


def parse_allmusic_recording_location(page: str) -> str:
    match = re.search(
        r'<div class="recording-location">\s*<h4>Recording Location</h4>\s*<div>(.*?)</div>',
        page,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return html_fragment_to_text(match.group(1)) if match else ""


def fetch_allmusic_review(album_url: str, timeout_s: int) -> Dict[str, str]:
    url = clean_text(album_url).rstrip("/")
    if not url:
        return {}

    try:
        response = http_get(
            f"{url}/reviewAjax",
            headers={"User-Agent": APP_USER_AGENT, "Referer": url},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] AllMusic 乐评抓取失败: {exc}")
        return {}

    page = response.text
    heading_match = re.search(r"<h3>(.*?)</h3>", page, flags=re.IGNORECASE | re.DOTALL)
    heading = html_fragment_to_text(heading_match.group(1)) if heading_match else "AllMusic Review"
    author = ""
    author_match = re.search(r"\bReview by\s+(.+)$", heading, flags=re.IGNORECASE)
    if author_match:
        author = clean_text(author_match.group(1))

    paragraphs = [
        html_fragment_to_text(match.group(1))
        for match in re.finditer(r"<p>(.*?)</p>", page, flags=re.IGNORECASE | re.DOTALL)
    ]
    review_text = clean_text(" ".join(paragraph for paragraph in paragraphs if paragraph))
    if len(review_text) < 120:
        return {}

    return {
        "source": "AllMusic",
        "author": author,
        "url": url,
        "text": review_text[:1400],
    }


def fetch_duckduckgo_results(query: str, timeout_s: int, limit: int = 5) -> List[Dict[str, str]]:
    try:
        response = http_get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] DuckDuckGo 搜索失败: {exc}")
        return []

    results: List[Dict[str, str]] = []
    for block in re.findall(r'<div class="result[\s\S]*?</div>\s*</div>', response.text):
        title_match = re.search(r'result__a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', block, flags=re.DOTALL)
        if not title_match:
            continue
        raw_url = html.unescape(title_match.group(1))
        parsed_url = urllib.parse.urlparse(raw_url)
        url = raw_url
        query_params = urllib.parse.parse_qs(parsed_url.query)
        if query_params.get("uddg"):
            url = query_params["uddg"][0]
        title = html_fragment_to_text(title_match.group(2))

        snippet = ""
        snippet_match = re.search(r'result__snippet[^>]*>(.*?)</a>', block, flags=re.DOTALL)
        if snippet_match:
            snippet = html_fragment_to_text(snippet_match.group(1))

        if title and url:
            results.append({"title": title, "url": clean_text(url), "snippet": snippet})
        if len(results) >= limit:
            break
    return results


def fetch_reader_markdown(url: str, timeout_s: int) -> str:
    clean_url = clean_text(url)
    if not clean_url:
        return ""
    reader_url = f"https://r.jina.ai/http://{clean_url}"
    try:
        response = http_get(reader_url, headers={"User-Agent": APP_USER_AGENT}, timeout=timeout_s)
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Reader 抓取失败: {clean_url}: {exc}")
        return ""
    text = clean_text(response.text)
    if "SecurityCompromiseError" in text or "Anonymous access" in text:
        return ""
    return text


def fetch_apple_music_editorial_notes(album: str, artist: str, timeout_s: int) -> Dict[str, str]:
    query = f'{album} {artist} site:music.apple.com/us/album'
    apple_url = ""
    for result in fetch_duckduckgo_results(query, timeout_s, limit=5):
        url = result.get("url", "")
        if "music.apple.com" in url and "/album/" in url:
            apple_url = url.split("?", 1)[0]
            break
    if not apple_url:
        return {}

    text = fetch_reader_markdown(apple_url, timeout_s)
    if not text:
        return {}

    note = ""
    patterns = [
        r"(?:Editors[’'] Notes|Editor[’']s Notes)\s*(.+?)(?:\s+(?:Song|Track)\s+Time\b|\s+#\s|\s+More By\b)",
        r"(?:Apple Music(?:\s+编辑)?(?:\s+注释| Review| Notes))\s*(.+?)(?:\s+(?:Song|Track)\s+Time\b|\s+#\s|\s+More By\b)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            note = clean_text(match.group(1))
            break

    generic_patterns = (
        "Listen to ",
        "Preview Song Time",
        "Open in Music",
        "Album by",
    )
    if len(note) < 160 or any(pattern in note[:120] for pattern in generic_patterns):
        return {}

    return {
        "source": "Apple Music",
        "author": "Apple Music Editors",
        "url": apple_url,
        "text": note[:1400],
    }


def fetch_genius_search_knowledge(album: str, artist: str, timeout_s: int) -> Dict[str, str]:
    query = f'"{album}" "{artist}" site:genius.com/albums'
    for result in fetch_duckduckgo_results(query, timeout_s, limit=5):
        url = result.get("url", "")
        snippet = clean_text(result.get("snippet", ""))
        title = clean_text(result.get("title", ""))
        if "genius.com" not in url:
            continue
        if len(snippet) < 140:
            continue
        if re.search(r"\blyrics\s+and\s+tracklist\b", snippet, flags=re.IGNORECASE) and len(snippet) < 220:
            continue
        if match_similarity(album, title) < 0.45 and match_similarity(album, snippet) < 0.45:
            continue
        return {
            "source": "Genius",
            "author": "",
            "url": url,
            "text": snippet[:800],
        }
    return {}


REVIEW_SITE_QUERIES = (
    ("Pitchfork", "pitchfork.com/reviews/albums", "site:pitchfork.com/reviews/albums"),
    ("Rolling Stone", "rollingstone.com", "site:rollingstone.com/music/music-album-reviews"),
    ("Album of the Year", "albumoftheyear.org", "site:albumoftheyear.org/album"),
    ("The Guardian", "theguardian.com", "site:theguardian.com/music"),
    ("NME", "nme.com", "site:nme.com/reviews/album"),
)


def fetch_music_review_site_knowledge(album: str, artist: str, timeout_s: int) -> List[Dict[str, str]]:
    review_items: List[Dict[str, str]] = []
    for source, domain, site_query in REVIEW_SITE_QUERIES:
        query = f'"{album}" "{artist}" {site_query}'
        for result in fetch_duckduckgo_results(query, timeout_s, limit=4):
            url = clean_text(result.get("url", ""))
            title = clean_text(result.get("title", ""))
            snippet = clean_text(result.get("snippet", ""))
            if domain not in url:
                continue
            if (
                match_similarity(album, title) < 0.38
                and match_similarity(album, snippet) < 0.38
                and clean_text(album).lower() not in f"{title} {snippet}".lower()
            ):
                continue
            text = clean_text(" ".join(part for part in (title, snippet) if part))
            if len(text) < 120:
                continue
            review_items.append(
                {
                    "source": source,
                    "author": "",
                    "url": url.split("?", 1)[0],
                    "text": text[:900],
                }
            )
            break
        if len(review_items) >= 3:
            break
    return review_items


def fetch_allmusic_release_group(album: str, artist: str, timeout_s: int) -> Optional[Dict[str, object]]:
    title = clean_text(album)
    performer = clean_text(artist)
    if not title or not performer:
        return None

    search_term = urllib.parse.quote(f"{title} {performer}")
    try:
        response = http_get(
            f"https://www.allmusic.com/search/albums/{search_term}",
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] AllMusic 搜索失败: {exc}")
        return None

    best_url = ""
    best_title = title
    best_artist = performer
    best_score = 0.0

    for match in re.finditer(r'<div class="album">\s*<div class="info">(.*?)<div class="cover">', response.text, re.DOTALL):
        block = match.group(1)
        title_match = re.search(r'<div class="title">\s*<a href="([^"]+)">(.+?)</a>', block, re.DOTALL)
        artist_match = re.search(r'<div class="artist">\s*<a [^>]*>(.+?)</a>', block, re.DOTALL)
        if not title_match or not artist_match:
            continue

        candidate_title = clean_text(html.unescape(re.sub(r"<[^>]+>", "", title_match.group(2))))
        candidate_artist = clean_text(html.unescape(re.sub(r"<[^>]+>", "", artist_match.group(1))))
        title_score = match_similarity(title, candidate_title)
        artist_score = match_similarity(performer, candidate_artist)
        combined = title_score + artist_score * 0.8
        if title_score < 0.72 or artist_score < 0.55 or combined <= best_score:
            continue

        best_score = combined
        best_url = clean_text(title_match.group(1))
        best_title = candidate_title
        best_artist = candidate_artist

    if not best_url:
        return None

    try:
        album_response = http_get(best_url, headers={"User-Agent": APP_USER_AGENT}, timeout=timeout_s)
        album_response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] AllMusic 专辑页抓取失败: {exc}")
        return None

    page = album_response.text
    release_date = ""
    genres: List[str] = []
    styles: List[str] = []
    summary = ""
    allmusic_rating = parse_allmusic_rating(page)
    recording_location = parse_allmusic_recording_location(page)
    review = fetch_allmusic_review(best_url, timeout_s)
    for pattern in (
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'data-release-date="([^"]+)"',
        r'<div class="release-date">\s*<h4>Release Date</h4>\s*<span>([^<]+)</span>',
    ):
        date_match = re.search(pattern, page, re.DOTALL)
        if not date_match:
            continue
        raw_date = clean_text(html.unescape(date_match.group(1)))
        release_date = raw_date if re.match(r"^\d{4}-\d{2}-\d{2}$", raw_date) else parse_human_date_to_iso(raw_date)
        if release_date:
            break

    genre_block = re.search(r'<div class="genre">\s*<h4>Genre</h4>\s*<div>(.*?)</div>', page, re.DOTALL)
    if genre_block:
        genres = unique_preserving_order(
            clean_text(html.unescape(re.sub(r"<[^>]+>", "", match.group(1))))
            for match in re.finditer(r"<a [^>]*>(.*?)</a>", genre_block.group(1), re.DOTALL)
        )[:3]

    styles_block = re.search(r'<div class="styles">\s*<h4>Styles</h4>\s*<div>(.*?)</div>', page, re.DOTALL)
    if styles_block:
        styles = unique_preserving_order(
            clean_text(html.unescape(re.sub(r"<[^>]+>", "", match.group(1))))
            for match in re.finditer(r"<a [^>]*>(.*?)</a>", styles_block.group(1), re.DOTALL)
        )[:4]

    meta_match = re.search(r'<meta name="description" content="([^"]+)"', page, re.DOTALL)
    if meta_match:
        summary = clean_text(html.unescape(meta_match.group(1)))

    evidence = build_today_echo_evidence(
        "AllMusic",
        best_title,
        best_artist,
        [release_date],
        best_url,
        genres=genres,
        styles=styles,
        summary=summary,
    )
    if evidence is not None:
        evidence["allmusic_rating"] = allmusic_rating
        evidence["recording_location"] = recording_location
        evidence["review"] = review
    return evidence


def fetch_wikidata_entity(entity_id: str, timeout_s: int) -> Optional[Dict[str, object]]:
    if not entity_id:
        return None
    try:
        response = http_get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json",
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
        return response.json().get("entities", {}).get(entity_id)
    except Exception as exc:
        print(f"[WARN] Wikidata 实体抓取失败: {exc}")
        return None


def fetch_wikidata_labels(entity_ids: Sequence[str], timeout_s: int) -> Dict[str, str]:
    ids = [entity_id for entity_id in unique_preserving_order(entity_ids) if entity_id]
    if not ids:
        return {}
    try:
        response = http_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": "|".join(ids),
                "format": "json",
                "props": "labels",
                "languages": "en",
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikidata 标签抓取失败: {exc}")
        return {}

    labels: Dict[str, str] = {}
    for entity_id, payload in response.json().get("entities", {}).items():
        label = clean_text(payload.get("labels", {}).get("en", {}).get("value", ""))
        if label:
            labels[entity_id] = label
    return labels


def extract_wikidata_release_dates(entity: Dict[str, object]) -> List[str]:
    release_dates: List[str] = []
    for claim in entity.get("claims", {}).get("P577", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if not isinstance(value, dict) or value.get("precision", 0) < 11:
            continue
        time_value = clean_text(value.get("time", ""))
        match = re.match(r"^\+?(\d{4}-\d{2}-\d{2})T", time_value)
        if match:
            release_dates.append(match.group(1))
    return unique_preserving_order(release_dates)


def extract_wikidata_entity_ids(entity: Dict[str, object], property_id: str) -> List[str]:
    entity_ids: List[str] = []
    for claim in entity.get("claims", {}).get(property_id, []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, dict):
            entity_id = clean_text(value.get("id", ""))
            if entity_id:
                entity_ids.append(entity_id)
    return unique_preserving_order(entity_ids)


def find_best_wikidata_album_entity(album: str, artist: str, timeout_s: int) -> Optional[Dict[str, object]]:
    title = clean_text(album)
    performer = clean_text(artist)
    if not title or not performer:
        return None

    try:
        response = http_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": title,
                "language": "en",
                "format": "json",
                "limit": 8,
                "type": "item",
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikidata 搜索失败: {exc}")
        return None

    search_hits = []
    for item in response.json().get("search", []):
        label = clean_text(item.get("label", ""))
        description = clean_text(item.get("description", ""))
        if "album" not in description.lower():
            continue
        title_score = match_similarity(title, label)
        if title_score < 0.72:
            continue
        search_hits.append((title_score, item))

    best_match: Optional[Dict[str, object]] = None
    best_score = 0.0

    for title_score, item in sorted(search_hits, key=lambda pair: pair[0], reverse=True)[:4]:
        entity_id = clean_text(item.get("id", ""))
        entity = fetch_wikidata_entity(entity_id, timeout_s)
        if not entity:
            continue

        performer_ids = extract_wikidata_entity_ids(entity, "P175")
        performer_labels = fetch_wikidata_labels(performer_ids, timeout_s)
        artist_score = max((match_similarity(performer, label) for label in performer_labels.values()), default=0.0)
        if artist_score < 0.55:
            description = clean_text(item.get("description", ""))
            artist_score = 0.6 if normalize_match_text(performer) in normalize_match_text(description) else artist_score
        if artist_score < 0.55:
            continue

        release_dates = extract_wikidata_release_dates(entity)
        if not release_dates:
            continue

        combined = title_score + artist_score * 0.8
        if combined <= best_score:
            continue

        best_score = combined
        best_match = {
            "entity_id": entity_id,
            "entity": entity,
            "album": clean_text(entity.get("labels", {}).get("en", {}).get("value", "")) or title,
            "artist": next(iter(performer_labels.values()), performer),
            "release_dates": release_dates,
            "url": f"https://www.wikidata.org/wiki/{entity_id}",
        }

    return best_match


def fetch_wikidata_release_group(album: str, artist: str, timeout_s: int) -> Optional[Dict[str, object]]:
    best_match = find_best_wikidata_album_entity(album, artist, timeout_s)
    if not best_match:
        return None
    return build_today_echo_evidence(
        "Wikidata",
        clean_text(best_match.get("album", "")) or clean_text(album),
        clean_text(best_match.get("artist", "")) or clean_text(artist),
        list(best_match.get("release_dates", [])),
        clean_text(best_match.get("url", "")),
    )


def fetch_wikidata_editorial_facts(album: str, artist: str, timeout_s: int) -> Dict[str, object]:
    match = find_best_wikidata_album_entity(album, artist, timeout_s)
    if not match:
        return {}

    entity = match.get("entity", {}) or {}
    genre_labels = fetch_wikidata_labels(extract_wikidata_entity_ids(entity, "P136"), timeout_s)
    label_labels = fetch_wikidata_labels(extract_wikidata_entity_ids(entity, "P264"), timeout_s)
    producer_labels = fetch_wikidata_labels(extract_wikidata_entity_ids(entity, "P162"), timeout_s)
    award_labels = fetch_wikidata_labels(extract_wikidata_entity_ids(entity, "P166"), timeout_s)

    return {
        "wikidata_url": clean_text(match.get("url", "")),
        "wikidata_genres": list(genre_labels.values())[:4],
        "wikidata_labels": list(label_labels.values())[:3],
        "wikidata_producers": list(producer_labels.values())[:3],
        "wikidata_awards": list(award_labels.values())[:3],
    }


def discover_today_echo_candidates_wikidata(timeout_s: int, limit: int = 25) -> List[Tuple[str, str]]:
    today = get_local_now()
    query = f"""
SELECT ?albumLabel ?artistLabel ?releaseDate WHERE {{
  ?album wdt:P31/wdt:P279* wd:Q482994;
         wdt:P577 ?releaseDate;
         wdt:P175 ?artist.
  FILTER(MONTH(?releaseDate) = {today.month} && DAY(?releaseDate) = {today.day})
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT {max(1, min(limit, 50))}
"""
    response = None
    last_error = None
    for attempt in range(3):
        try:
            response = http_get(
                "https://query.wikidata.org/sparql",
                params={"query": query, "format": "json"},
                headers={
                    "User-Agent": APP_USER_AGENT,
                    "Accept": "application/sparql-results+json",
                },
                timeout=timeout_s,
            )
            response.raise_for_status()
            break
        except Exception as exc:
            last_error = exc
            time.sleep(min(6, 1 + attempt * 2))
    if response is None:
        print(f"[WARN] Wikidata 当日专辑候选抓取失败: {last_error}")
        return []

    pairs: List[Tuple[str, str]] = []
    for row in response.json().get("results", {}).get("bindings", []):
        album_label = clean_text(row.get("albumLabel", {}).get("value", ""))
        artist_label = clean_text(row.get("artistLabel", {}).get("value", ""))
        if album_label and artist_label and (album_label, artist_label) not in pairs:
            pairs.append((album_label, artist_label))
    return pairs


def collect_today_echo_evidence(album: str, artist: str, timeout_s: int) -> List[Dict[str, object]]:
    evidence: List[Dict[str, object]] = []
    fetchers = (
        fetch_musicbrainz_release_group,
        fetch_allmusic_release_group,
        fetch_wikidata_release_group,
    )
    with ThreadPoolExecutor(max_workers=len(fetchers)) as executor:
        future_map = {
            executor.submit(fetcher, album, artist, timeout_s): fetcher.__name__
            for fetcher in fetchers
        }
        for future in as_completed(future_map):
            try:
                result = future.result()
            except Exception as exc:
                print(f"[WARN] 今日回响外部验证失败: {exc}")
                result = None
            if result:
                evidence.append(result)
    return evidence


def verify_today_echo_candidate(album: str, artist: str, timeout_s: int) -> Optional[Dict[str, object]]:
    evidence = collect_today_echo_evidence(album, artist, timeout_s)
    if not evidence:
        return None

    date_scores: Dict[str, int] = defaultdict(int)
    date_sources: Dict[str, List[str]] = defaultdict(list)
    today_md = get_today_month_day()

    for item in evidence:
        source = clean_text(item.get("source", ""))
        for release_date in item.get("release_dates", []):
            release_dt = parse_iso_date(release_date)
            if not release_dt or release_dt.strftime("%m-%d") != today_md:
                continue
            date_scores[release_date] += TODAY_ECHO_SOURCE_WEIGHTS.get(source, 1)
            date_sources[release_date].append(source)

    best_date = ""
    best_score = 0
    best_source_count = 0
    for release_date, score in date_scores.items():
        source_count = len(set(date_sources[release_date]))
        if source_count < TODAY_ECHO_MIN_SOURCE_COUNT or score < TODAY_ECHO_MIN_CONSENSUS_SCORE:
            continue
        if (score, source_count, release_date) > (best_score, best_source_count, best_date):
            best_date = release_date
            best_score = score
            best_source_count = source_count

    if not best_date:
        return None

    canonical_album = clean_text(album)
    canonical_artist = clean_text(artist)
    genre_hints: List[str] = []
    for item in evidence:
        if best_date not in item.get("release_dates", []):
            continue
        if match_similarity(item.get("album", ""), canonical_album) > 0.75:
            canonical_album = clean_text(item.get("album", "")) or canonical_album
        if match_similarity(item.get("artist", ""), canonical_artist) > 0.65:
            canonical_artist = clean_text(item.get("artist", "")) or canonical_artist
        genre_hints.extend(item.get("genres", []))

    return {
        "album": canonical_album,
        "artist": canonical_artist,
        "release_date": best_date,
        "verification_sources": unique_preserving_order(date_sources[best_date]),
        "verification_score": best_score,
        "genre_hint": " / ".join(unique_preserving_order(genre_hints)[:3]),
        "evidence": evidence,
    }


def request_today_echo_candidates() -> List[Tuple[str, str]]:
    today_md = get_today_month_day()
    prompt = f"""
你是音乐史编辑。今天的现实日期月日是 {today_md}。

请只返回 JSON 对象，格式如下：
{{
  "candidates": [
    {{"album": "专辑名", "artist": "艺人名"}},
    {{"album": "专辑名", "artist": "艺人名"}}
  ]
}}

规则：
1. 至少给出 8 个候选。
2. 只列出你认为“官方首发月日”与今天相同的录音室专辑。
3. 优先给出历史地位高、评论口碑强、信息更容易验证的专辑。
4. 不要解释，不要 Markdown，不要额外字段。
"""
    data = extract_json_object(call_llm(prompt))
    pairs: List[Tuple[str, str]] = []
    for item in data.get("candidates", []):
        if not isinstance(item, dict):
            continue
        album = clean_text(item.get("album", ""))
        artist = clean_text(item.get("artist", ""))
        if album and artist and (album, artist) not in pairs:
            pairs.append((album, artist))
    return pairs


def select_verified_today_echo_candidate(timeout_s: int, allow_llm: bool = True) -> Optional[Dict[str, object]]:
    checked = set()
    best_match: Optional[Dict[str, object]] = None

    candidate_batches: List[List[Tuple[str, str]]] = []
    if allow_llm:
        for _ in range(2):
            try:
                candidate_batches.append(request_today_echo_candidates())
            except Exception as exc:
                print(f"[WARN] 今日回响候选生成失败: {exc}")
    candidate_batches.append(discover_today_echo_candidates_wikidata(timeout_s, limit=8 if not allow_llm else 16))

    for candidates in candidate_batches:
        for album, artist in candidates:
            key = (album, artist)
            if key in checked:
                continue
            checked.add(key)
            verified = verify_today_echo_candidate(album, artist, timeout_s)
            if not verified:
                continue

            candidate_rank = (
                int(verified.get("verification_score", 0)),
                len(verified.get("verification_sources", [])),
                match_similarity(verified.get("album", ""), album),
            )
            best_rank = (
                int(best_match.get("verification_score", 0)),
                len(best_match.get("verification_sources", [])),
                0.0,
            ) if best_match else (-1, -1, -1.0)
            if candidate_rank > best_rank:
                best_match = verified
                if (
                    int(best_match.get("verification_score", 0)) >= TODAY_ECHO_MIN_CONSENSUS_SCORE
                    and len(best_match.get("verification_sources", [])) >= TODAY_ECHO_MIN_SOURCE_COUNT
                ):
                    return best_match

    return best_match


def fetch_wikipedia_album_summary(album: str, artist: str, timeout_s: int) -> Dict[str, str]:
    title = clean_text(album)
    performer = clean_text(artist)
    if not title or not performer:
        return {}

    try:
        response = http_get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": f'{title} "{performer}" album',
                "format": "json",
                "srlimit": 5,
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikipedia 专辑搜索失败: {exc}")
        return {}

    best_title = ""
    best_score = 0.0
    for item in response.json().get("query", {}).get("search", []):
        candidate_title = clean_text(item.get("title", ""))
        snippet = clean_text(item.get("snippet", ""))
        score = match_similarity(title, candidate_title)
        if "album" in snippet.lower():
            score += 0.2
        if performer.lower() in snippet.lower():
            score += 0.15
        if score > best_score:
            best_score = score
            best_title = candidate_title

    if best_score < 0.72 or not best_title:
        return {}

    try:
        summary_response = http_get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(best_title, safe='')}",
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        summary_response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikipedia 专辑摘要抓取失败: {exc}")
        return {}

    payload = summary_response.json()
    extract = clean_text(payload.get("extract", ""))
    if not extract:
        return {}
    return {
        "source": "Wikipedia",
        "title": best_title,
        "summary": extract,
        "url": clean_text(payload.get("content_urls", {}).get("desktop", {}).get("page", "")),
    }


def fetch_wikipedia_album_section_text(page_title: str, section_names: Sequence[str], timeout_s: int) -> Dict[str, str]:
    title = clean_text(page_title)
    if not title:
        return {}

    try:
        sections_response = http_get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "parse",
                "page": title,
                "prop": "sections",
                "format": "json",
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        sections_response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikipedia 专辑章节索引抓取失败: {exc}")
        return {}

    section_index = ""
    wanted = {name.lower() for name in section_names}
    for section in sections_response.json().get("parse", {}).get("sections", []):
        line = clean_text(section.get("line", ""))
        if line.lower() in wanted:
            section_index = clean_text(section.get("index", ""))
            break
    if not section_index:
        return {}

    try:
        text_response = http_get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "parse",
                "page": title,
                "section": section_index,
                "prop": "text",
                "format": "json",
            },
            headers={"User-Agent": APP_USER_AGENT},
            timeout=timeout_s,
        )
        text_response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Wikipedia 专辑章节抓取失败: {exc}")
        return {}

    raw_html = text_response.json().get("parse", {}).get("text", {}).get("*", "")
    text = html_fragment_to_text(raw_html)
    text = re.sub(r"^(Critical reception|Reception)\s*(?:\[\s*edit\s*\])?\s*", "", text, flags=re.IGNORECASE)
    text = re.split(r"\s\^\s|\sRetrieved\s", text)[0].strip()
    if len(text) < 120:
        return {}

    return {
        "source": "Wikipedia Critical reception",
        "url": f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title.replace(' ', '_'))}",
        "text": text[:1400],
    }


def collect_today_echo_editorial_facts(album: str, artist: str, timeout_s: int) -> Dict[str, object]:
    facts: Dict[str, object] = {
        "genres": [],
        "styles": [],
        "allmusic_summary": "",
        "allmusic_url": "",
        "allmusic_rating": "",
        "wikipedia_summary": "",
        "wikipedia_url": "",
        "wikidata_genres": [],
        "wikidata_labels": [],
        "wikidata_producers": [],
        "wikidata_awards": [],
        "wikidata_url": "",
        "recording_locations": [],
        "review_sources": [],
    }

    allmusic = fetch_allmusic_release_group(album, artist, timeout_s)
    if allmusic:
        facts["genres"] = allmusic.get("genres", [])
        facts["styles"] = allmusic.get("styles", [])
        facts["allmusic_summary"] = allmusic.get("summary", "")
        facts["allmusic_url"] = allmusic.get("url", "")
        facts["allmusic_rating"] = allmusic.get("allmusic_rating", "")
        facts["recording_locations"] = unique_preserving_order([allmusic.get("recording_location", "")])
        if allmusic.get("review"):
            facts["review_sources"].append(allmusic["review"])

    wikipedia = fetch_wikipedia_album_summary(album, artist, timeout_s)
    if wikipedia:
        facts["wikipedia_summary"] = wikipedia.get("summary", "")
        facts["wikipedia_url"] = wikipedia.get("url", "")
        reception = fetch_wikipedia_album_section_text(
            wikipedia.get("title", ""),
            ("Critical reception", "Reception"),
            timeout_s,
        )
        if reception:
            facts["review_sources"].append(reception)

    apple_music = fetch_apple_music_editorial_notes(album, artist, timeout_s)
    if apple_music:
        facts["review_sources"].append(apple_music)

    genius = fetch_genius_search_knowledge(album, artist, timeout_s)
    if genius:
        facts["review_sources"].append(genius)

    for review_item in fetch_music_review_site_knowledge(album, artist, timeout_s):
        facts["review_sources"].append(review_item)

    wikidata = fetch_wikidata_editorial_facts(album, artist, timeout_s)
    if wikidata:
        facts["wikidata_genres"] = wikidata.get("wikidata_genres", [])
        facts["wikidata_labels"] = wikidata.get("wikidata_labels", [])
        facts["wikidata_producers"] = wikidata.get("wikidata_producers", [])
        facts["wikidata_awards"] = wikidata.get("wikidata_awards", [])
        facts["wikidata_url"] = wikidata.get("wikidata_url", "")

    return facts


def ranked_today_echo_review_sources(review_sources: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    priority = {
        "Apple Music": 0,
        "Pitchfork": 1,
        "Rolling Stone": 2,
        "Album of the Year": 3,
        "The Guardian": 4,
        "NME": 5,
        "AllMusic": 6,
        "Genius": 7,
        "Wikipedia Critical reception": 8,
    }
    return sorted(
        list(review_sources),
        key=lambda item: (
            priority.get(clean_text(str(item.get("source", ""))), 20),
            -len(clean_text(str(item.get("text", "")))),
        ),
    )


def today_echo_note_is_acceptable(note: str) -> bool:
    text = clean_text(note)
    if len(text) < 80:
        return False
    if len(text) > 180:
        return False
    banned_phrases = [
        "适合作为当天的音乐史回响",
        "稳定的历史地位与持续讨论度",
        "优先采用外部元数据",
        "本期在保证日期准确的前提下",
        "首发日期",
        "交叉验证",
        "当前可核查资料",
        "基础作品信息",
        "本期只保留",
        "提供了专辑层面的评论证据",
        "推荐理由应从评论论点进入",
        "推荐理由应",
        "页面标注",
        "评价焦点落在",
        "资料清单",
        "好听",
        "杰作",
        "神作",
        "伟大",
        "完美",
        "震撼",
        "不可错过",
        "封神",
    ]
    if any(phrase in text for phrase in banned_phrases):
        return False
    if text.count("；") >= 4:
        return False
    substantive_music_fact_markers = (
        "录音技术", "录音师", "录制", "制作", "制作人", "编曲", "采样", "混音", "母带",
        "榜单", "Billboard", "奖", "格莱美", "Grammy", "水星奖", "普利策",
        "历史地位", "排名", "年度", "十年", "评分", "星级", "满分", "单曲", "专辑榜",
    )
    music_criticism_markers = (
        "声音", "音色", "声场", "人声", "吉他", "贝斯", "鼓", "键盘", "合成器", "弦乐",
        "旋律", "节奏", "律动", "和声", "编曲", "结构", "叙事", "歌词", "制作", "录音",
        "混音", "母带", "采样", "曲式", "合奏", "氛围", "乐队", "器乐", "唱腔", "段落",
    )
    has_music_criticism = sum(1 for marker in music_criticism_markers if marker in text) >= 2
    has_substantive_fact = any(marker in text for marker in substantive_music_fact_markers)
    has_review_source_context = (
        any(source in text for source in ("AllMusic", "Pitchfork", "Rolling Stone", "Apple Music", "Genius", "Album of the Year"))
        and any(marker in text for marker in ("评论", "评价", "评分", "星", "认为", "称", "写道", "指出", "强调", "注释"))
    )
    metadata_smell_phrases = [
        "将其归入",
        "将其界定为",
        "关联的发行厂牌",
        "交叉验证",
        "关联厂牌",
        "补足唱片工业语境",
        "并标注",
        "等 Styles 标签",
        "评论证据",
    ]
    metadata_smell_count = sum(text.count(phrase) for phrase in metadata_smell_phrases)
    if metadata_smell_count >= 2:
        return False

    return has_music_criticism or has_substantive_fact or has_review_source_context


ORDINAL_CN_MAP = {
    "first": "第一", "second": "第二", "third": "第三", "fourth": "第四", "fifth": "第五",
    "sixth": "第六", "seventh": "第七", "eighth": "第八", "ninth": "第九", "tenth": "第十",
    "eleventh": "第十一", "twelfth": "第十二", "thirteenth": "第十三", "fourteenth": "第十四",
    "fifteenth": "第十五",
}


def build_wikipedia_album_fact(summary: str, artist: str) -> str:
    text = clean_text(summary)
    if not text:
        return ""

    match = re.search(r"\bis the ([a-z-]+) studio album by\b", text, re.IGNORECASE)
    if match:
        ordinal = ORDINAL_CN_MAP.get(match.group(1).lower(), "")
        if ordinal:
            return f"Wikipedia 将其界定为 {artist} 的{ordinal}张录音室专辑"
        return f"Wikipedia 将其界定为 {artist} 的一张录音室专辑"

    if re.search(r"\bstudio album by\b", text, re.IGNORECASE):
        return f"Wikipedia 将其界定为 {artist} 的一张录音室专辑"
    if re.search(r"\blive album by\b", text, re.IGNORECASE):
        return f"Wikipedia 将其界定为 {artist} 的一张现场专辑"
    return ""


def format_fact_list(values: Sequence[str], limit: int = 3) -> str:
    cleaned = unique_preserving_order(values)[:limit]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return "、".join(cleaned[:-1]) + f" 与 {cleaned[-1]}"


def truncate_review_note_at_sentence(note: str, limit: int = 180) -> str:
    text = clean_text(note)
    if len(text) <= limit:
        return text

    kept = ""
    for sentence in re.findall(r"[^。！？!?]+[。！？!?]", text):
        if kept and len(kept) + len(sentence) > limit:
            break
        if not kept and len(sentence) > limit:
            break
        kept += sentence
    if kept:
        return kept

    return ensure_terminal_punctuation(text[:limit].rstrip("，,；;：: "))


def build_review_source_fallback_note(
    album: str,
    editorial_facts: Dict[str, object],
    fast_genre: str,
) -> str:
    review_sources = ranked_today_echo_review_sources(list(editorial_facts.get("review_sources", [])))
    review = next((item for item in review_sources if clean_text(str(item.get("text", "")))), None)
    if not review:
        return ""

    review_text = clean_text(str(review.get("text", "")))
    source = clean_text(str(review.get("source", "")))
    author = clean_text(str(review.get("author", "")))
    rating = clean_text(str(editorial_facts.get("allmusic_rating", "")))
    recording_locations = list(editorial_facts.get("recording_locations", []))
    recording_location = clean_text(str(recording_locations[0])) if recording_locations else ""

    arrangement_facts: List[str] = []
    marker_map = [
        (r"\bkeyboards?\b|键盘", "键盘"),
        (r"\bguitars?\b|吉他", "吉他"),
        (r"\bvoices?\b|\bvocals?\b|人声|唱腔", "人声"),
        (r"\bdrums?\b|鼓|节奏", "节奏"),
        (r"\bsynth(?:esizer)?s?\b|合成器", "合成器"),
        (r"\blyrics?\b|歌词", "歌词"),
        (r"\bproduction\b|制作", "制作"),
        (r"\brecord(?:ing|ed)?\b|录音", "录音"),
    ]
    for pattern, label in marker_map:
        if re.search(pattern, review_text, re.IGNORECASE):
            arrangement_facts.append(label)

    narrative_subject = ""
    if re.search(r"\bwarhol\b", review_text, re.IGNORECASE):
        narrative_subject = "Warhol 生平"
    arrangement_text = (
        "、".join(arrangement_facts[:-1]) + f"和{arrangement_facts[-1]}"
        if len(arrangement_facts) >= 2
        else format_fact_list(arrangement_facts)
    )
    if (
        re.search(r"\bcale\b", review_text, re.IGNORECASE)
        and re.search(r"\breed\b", review_text, re.IGNORECASE)
        and {"键盘", "吉他", "人声"}.issubset(set(arrangement_facts))
    ):
        arrangement_text = "Cale 的键盘、Reed 的吉他和两人的人声"

    if len(arrangement_facts) >= 2 and narrative_subject:
        first = f"《{album}》以{arrangement_text}构成紧凑骨架，并围绕 {narrative_subject}展开叙事。"
    elif len(arrangement_facts) >= 2:
        first = f"《{album}》的听感重心落在{arrangement_text}的相互牵引上，歌曲不是风格标签的堆叠，而是靠编制关系推进。"
    elif review_text:
        first = f"《{album}》更值得从歌曲结构、编制和录音空间的关系切入，而不是只按流派名词归类。"
    else:
        return ""

    supplements = []
    if source == "AllMusic" and author:
        supplements.append(f"{author} 的专辑评论")
    elif source == "Apple Music":
        supplements.append("Apple Music 编辑语的作品脉络")
    elif source:
        supplements.append(f"{source} 的评论线索")
    if source == "AllMusic" and rating:
        supplements.append(f"{rating} 评分")
    if recording_location:
        location_text = re.sub(r"^(.+),\s*([^,]+,\s*[^,]+)$", r"\1（\2）", recording_location)
        supplements.append(f"{location_text}录音地点")
    if not supplements and fast_genre:
        supplements.append(f"{fast_genre} 风格语境")

    if supplements:
        second = f"{'与 '.join(supplements)}把这张唱片的工业语境落到可核查层面；声音组织和作品位置才是判断它的核心。"
    else:
        second = "这类专辑的判断重点在声音组织、歌曲结构和艺人阶段，而不是停留在风格名词。"

    note = truncate_review_note_at_sentence(f"{first}{second}", limit=180)
    return ensure_terminal_punctuation(note)


def build_local_today_echo_note(
    album: str,
    artist: str,
    release_date: str,
    source_line: str,
    editorial_facts: Dict[str, object],
    fast_genre: str,
) -> str:
    genres = format_fact_list(editorial_facts.get("genres", []), 2)
    styles = format_fact_list(editorial_facts.get("styles", []), 3)
    wikidata_genres = format_fact_list(editorial_facts.get("wikidata_genres", []), 3)
    producers = "、".join(editorial_facts.get("wikidata_producers", [])[:2])
    labels = "、".join(editorial_facts.get("wikidata_labels", [])[:2])
    awards = "、".join(editorial_facts.get("wikidata_awards", [])[:2])

    sentences: List[str] = []
    if genres or styles:
        style_part = f"，并列出 {styles} 等 Styles 标签" if styles else ""
        sentences.append(
            f"AllMusic 把《{album}》归入 {genres or fast_genre}{style_part}。"
        )
    elif wikidata_genres:
        sentences.append(
            f"Wikidata 将《{album}》关联到 {wikidata_genres} 等流派，为判断作品谱系提供了可核查入口。"
        )

    wiki_fact = build_wikipedia_album_fact(str(editorial_facts.get("wikipedia_summary", "")), artist)
    if wiki_fact:
        sentences.append(f"{wiki_fact}。")

    if producers:
        sentences.append(f"Wikidata 记录制作人包括 {producers}，制作班底成为可核查线索。")
    elif labels:
        sentences.append(f"Wikidata 关联厂牌 {labels}，补足唱片工业语境。")

    if awards:
        sentences.append(f"Wikidata 记录其关联过 {awards}，说明它具备可追踪的机构评价记录。")

    note = "".join(sentence for sentence in sentences if sentence).strip()
    note = truncate_review_note_at_sentence(note, limit=180)
    return ensure_terminal_punctuation(note)


def build_knowledge_based_today_echo_note(album: str, artist: str, fast_genre: str) -> str:
    prompt = f"""
你是严谨的中文音乐评论编辑。请参考 Apple Music 编辑团队的专辑推荐语气质，凭音乐史知识为以下专辑写一段 100-150 字的简体中文短乐评。

专辑：{album}
艺人：{artist}
流派提示：{fast_genre or '未知'}

只返回 JSON 对象：
{{
  "note": "100-150 字简体中文连续乐评"
}}

硬规则：
1. 写成连续段落，不要写成资料清单、字段汇总或元数据罗列。
2. 从声音结构、制作/录音语境、歌曲叙事、艺人阶段、历史影响或流派作用切入，读起来像一段真正的编辑推荐。
3. 禁止只写“AllMusic 流派/Styles 标签”“Wikipedia 录音室专辑”“Wikidata 厂牌/发行信息”。
4. 禁止“好听”“经典”“杰作”“神作”“伟大”“完美”“震撼”“不可错过”“封神”等浮夸词。
5. 不要为了显得有依据而强行写 AllMusic / Pitchfork / Rolling Stone / Apple Music / Album of the Year 等来源名；只有确有把握且自然时才提。
6. 若不确定具体评分、奖项、榜单名或媒体原话，不要编造；用可稳妥概括的音乐史判断替代。
7. 不要输出 JSON 以外的任何内容。
"""
    try:
        data = extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] 今日回响知识型乐评生成失败: {exc}")
        return ""
    return clean_text(data.get("note", ""))


def build_today_echo_pause_note() -> str:
    return (
        "现有 AllMusic / Pitchfork / Rolling Stone / Apple Music / Genius / Wikipedia / Wikidata 音乐事实不足以支撑一段符合质量红线的专业乐评；"
        "本期暂停推荐，避免用风格标签、发行信息或厂牌数据拼接成空泛段落。"
    )


def repair_today_echo_note_with_llm(note: str, evidence_lines: Sequence[str]) -> str:
    prompt = f"""
你是中文音乐编辑。请把下面这段“今日回响”乐评压缩改写为 100-150 个汉字。

原文：
{clean_text(note)}

可用证据：
{chr(10).join(evidence_lines)}

只返回 JSON 对象：
{{
  "note": "100-150 个汉字的中文乐评"
}}

硬规则：
1. 必须保留至少 2 条可验证事实或评论判断，优先保留 Apple Music 编辑推荐、Pitchfork / Rolling Stone / Album of the Year / AllMusic 等评论论点、录音地点、制作/录音/榜单/奖项事实。
2. 禁止“杰作”“神作”“伟大”“完美”“震撼”“不可错过”等浮夸词。
3. 禁止写日期验证、资料不足、适合作为今日回响等解释。
4. 不要求在正文里点名任何媒体或评分；媒体名和评分只是证据，不是必须出现的文案元素。
5. 禁止照搬英文原句；只做中文转述。
6. 不要输出 JSON 以外的任何内容。
"""
    try:
        data = extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] 今日回响乐评压缩失败: {exc}")
        return ""
    return clean_text(data.get("note", ""))


def build_today_echo_note(
    album: str,
    artist: str,
    release_date: str,
    verification_sources: Optional[Sequence[str]] = None,
    genre_hint: str = "",
    timeout_s: int = 15,
) -> Tuple[str, str]:
    source_line = " / ".join(unique_preserving_order(verification_sources or [])) or "MusicBrainz / AllMusic / Wikidata"
    editorial_facts = collect_today_echo_editorial_facts(album, artist, timeout_s)
    styles = " / ".join(editorial_facts.get("styles", [])[:3])
    genres = " / ".join(editorial_facts.get("genres", [])[:2])
    fast_genre = clean_text(genre_hint) or styles or genres or "流行 / 摇滚"
    review_sources = ranked_today_echo_review_sources(list(editorial_facts.get("review_sources", [])))
    review_payload = [
        {
            "source": clean_text(str(item.get("source", ""))),
            "author": clean_text(str(item.get("author", ""))),
            "url": clean_text(str(item.get("url", ""))),
            "text": clean_text(str(item.get("text", "")))[:1200],
        }
        for item in review_sources[:5]
        if clean_text(str(item.get("text", "")))
    ]
    evidence_lines = [
        f"- 日期交叉验证来源：{source_line}",
        f"- AllMusic 流派：{genres or '未知'}",
        f"- AllMusic 风格：{styles or '未知'}",
        f"- AllMusic 评分：{editorial_facts.get('allmusic_rating', '') or '未知'}",
        f"- 录音地点：{' / '.join(editorial_facts.get('recording_locations', [])) or '未知'}",
        f"- AllMusic 页面摘要：{editorial_facts.get('allmusic_summary', '') or '无'}",
        f"- Wikipedia 摘要：{editorial_facts.get('wikipedia_summary', '') or '无'}",
        f"- Wikidata 流派：{' / '.join(editorial_facts.get('wikidata_genres', [])[:3]) or '无'}",
        f"- Wikidata 制作人：{' / '.join(editorial_facts.get('wikidata_producers', [])[:3]) or '无'}",
        f"- Wikidata 厂牌：{' / '.join(editorial_facts.get('wikidata_labels', [])[:3]) or '无'}",
        f"- Wikidata 奖项：{' / '.join(editorial_facts.get('wikidata_awards', [])[:3]) or '无'}",
        f"- 专业乐评证据池（按优先级：Apple Music 编辑推荐语、Pitchfork、Rolling Stone、Album of the Year、The Guardian、NME、AllMusic、Genius、Wikipedia reception）：{json.dumps(review_payload, ensure_ascii=False)}",
    ]

    prompt = f"""
你是严谨的音乐主编兼乐评编辑。请参考 Apple Music 编辑团队的专辑推荐语气质，把外部乐评和音乐事实改写成一段流利、克制、有判断的中文短乐评。

以下信息已经通过多源交叉验证：
- 专辑：{album}
- 艺人：{artist}
- 官方首发日期：{release_date}
- 证据：
{chr(10).join(evidence_lines)}

请只返回 JSON 对象：
{{
  "genre": "不超过 3 个短流派，用 / 连接",
  "note": "100-150 字中文乐评"
}}

规则：
1. note 必须是简体中文，克制、准确、专业，写成连续乐评短段，不要写成“资料清单”。
2. 绝对禁止写“适合作为今天回响”“历史地位稳定”“持续讨论度”“首发日期已验证”“当前可核查资料”等空话或校验说明。
3. 必须优先基于“专业乐评证据池”汇总；若 Apple Music 编辑推荐语存在，优先吸收其叙述角度和编辑语气，但不要照搬。
4. Pitchfork / Rolling Stone / Album of the Year / AllMusic / Genius 等只是可用证据来源；正文不必强行点名任何媒体、评分或作者，除非这样写自然且有助于表达。
5. 若证据池没有可用乐评，再基于录音室技术、制作班底、历史评分地位、奖项、榜单、重要单曲等硬事实写作。
6. 只写“流派 / 风格标签 / 录音室专辑 / 发行厂牌 / 首发日期”不合格；这些只能作为辅助事实，不能作为推荐理由主体。
7. 不要把句子写成“某来源将其归入/界定为/记录为”的罗列；要把事实转译成关于声音、结构、叙事、制作或艺人阶段的专业判断。
8. 禁止照搬英文评论原句；只能中文转述与压缩，不要连续引用超过 10 个英文词。
9. note 必须控制在 100-150 个汉字，超过 180 个汉字视为失败。
10. 禁止“杰作”“神作”“伟大”“完美”“震撼”“不可错过”等浮夸词。
11. 不要改动专辑名、艺人名和首发日期。
12. 不要输出 JSON 以外的任何内容。
"""
    try:
        data = extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] 今日回响乐评生成失败: {exc}")
        data = {}

    genre = clean_text(data.get("genre", "")) or fast_genre
    note = clean_text(data.get("note", ""))
    if note and not today_echo_note_is_acceptable(note) and review_payload:
        repaired_note = repair_today_echo_note_with_llm(note, evidence_lines)
        if today_echo_note_is_acceptable(repaired_note):
            note = repaired_note
    if not today_echo_note_is_acceptable(note) and review_payload:
        fallback_note = build_review_source_fallback_note(album, editorial_facts, fast_genre)
        if today_echo_note_is_acceptable(fallback_note):
            note = fallback_note
    if not today_echo_note_is_acceptable(note):
        note = build_knowledge_based_today_echo_note(album, artist, fast_genre)
    if not today_echo_note_is_acceptable(note):
        note = build_today_echo_pause_note()
    return genre, note


def render_today_echo_section(album: str, artist: str, release_date: str, genre: str, note: str) -> str:
    safe_album = clean_text(album)
    safe_artist = clean_text(artist)
    safe_date = clean_text(release_date)
    safe_genre = clean_text(genre)
    safe_note = clean_text(note)
    return "\n".join(
        [
            "---",
            "🎵 **今日回响**",
            f"### 《{safe_album}》",
            f"> 🎤 **{safe_artist}** · 📅 {safe_date} · 💽 {safe_genre}",
            ">",
            "> **📝 编辑推荐 / Editor's Notes：**",
            f"> {safe_note}",
        ]
    )


def replace_today_echo_section(md_text: str, section_md: str) -> str:
    lines = md_text.splitlines()
    start = find_today_echo_start(lines)
    if start is None:
        base = md_text.rstrip()
        spacer = "\n\n" if base else ""
        return f"{base}{spacer}{section_md}".strip()
    prefix = "\n".join(lines[:start]).rstrip()
    spacer = "\n\n" if prefix else ""
    return f"{prefix}{spacer}{section_md}".strip()


def ensure_verified_today_echo(md_text: str, timeout_s: int) -> str:
    echo_timeout = get_today_echo_timeout(timeout_s)
    existing = extract_today_echo_metadata(md_text)
    verified = None

    if existing.get("album") and existing.get("artist"):
        verified = verify_today_echo_candidate(existing["album"], existing["artist"], echo_timeout)
        if verified:
            print(
                f"[INFO] 今日回响日期已通过多源验证: "
                f"{verified['album']} / {verified['release_date']} / {', '.join(verified.get('verification_sources', []))}"
            )
        else:
            print(
                f"[WARN] 今日回响候选未通过日期验证: "
                f"{existing.get('album', '')} / {existing.get('artist', '')} / {existing.get('release_date', '')}"
            )

    if not verified:
        if should_avoid_heavy_llm_repairs():
            print("[INFO] 今日回响改为保守模式：不额外请求模型找新专辑，但会继续使用 Wikidata / MusicBrainz / AllMusic 进行外部候选发现与交叉验证。")
            verified = select_verified_today_echo_candidate(echo_timeout, allow_llm=False)
        else:
            verified = select_verified_today_echo_candidate(echo_timeout)
        if verified:
            print(
                f"[INFO] 今日回响已替换为经验证专辑: "
                f"{verified['album']} / {verified['release_date']} / {', '.join(verified.get('verification_sources', []))}"
            )

    if not verified:
        print(f"[WARN] 未找到与今天（{get_today_month_day()}）匹配的经验证专辑，今日回响将暂停推荐。")
        fallback = "\n".join(
            [
                "---",
                "🎵 **今日回响**",
                f"> 已综合 MusicBrainz、AllMusic、Wikidata 等来源交叉核查，但仍未找到首发月日与今天（{get_today_month_day()}）一致且能形成多源共识的专辑，本期暂停推荐，以避免错误日期。",
            ]
        )
        return replace_today_echo_section(md_text, fallback)

    genre, note = build_today_echo_note(
        verified["album"],
        verified["artist"],
        verified["release_date"],
        verified.get("verification_sources", []),
        verified.get("genre_hint", ""),
        echo_timeout,
    )
    section_md = render_today_echo_section(
        verified["album"],
        verified["artist"],
        verified["release_date"],
        genre,
        note,
    )
    return replace_today_echo_section(md_text, section_md)


def strip_md_wrappers(text: str) -> str:
    value = text.strip()
    value = re.sub(r"^#+\s*", "", value)
    for _ in range(3):
        value = re.sub(r"^\*\*(.+)\*\*$", r"\1", value)
        value = re.sub(r"^\*(.+)\*$", r"\1", value)
        value = re.sub(r"^_(.+)_$", r"\1", value)
        value = re.sub(r"^`(.+)`$", r"\1", value)
    return value.strip()


def strip_list_prefix(text: str) -> str:
    return re.sub(r"^(?:\d+\.|[-*])\s+", "", text.strip())


def split_label_and_body(text: str) -> Tuple[str, str]:
    candidate = strip_list_prefix(text)
    strong_match = re.match(r"^\*\*(.+?)\*\*(?:[:：]\s*(.*))?$", candidate)
    if strong_match:
        return strong_match.group(1).strip(), (strong_match.group(2) or "").strip()

    plain_match = re.match(r"^([^:：]+)[:：]\s*(.*)$", candidate)
    if plain_match:
        return plain_match.group(1).strip(), plain_match.group(2).strip()
    return strip_md_wrappers(candidate), ""


def clean_label_text(text: str) -> str:
    value = strip_md_wrappers(text)
    value = re.sub(r"[*_`]+", "", value).strip()
    value = re.sub(r"^\[(.+)\]\s*(视角.*)$", r"\1\2", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def contains_cjk(text: str) -> bool:
    return re.search(r"[\u3400-\u9fff]", text or "") is not None


def strip_link_markup(text: str) -> str:
    value = text or ""
    value = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", value)
    value = re.sub(r"<a\s+href=\"[^\"]+\">([^<]+)</a>", r"\1", value)
    value = re.sub(r"https?://\S+", "", value)
    return clean_text(value)


def contains_substantial_english(text: str) -> bool:
    candidate = strip_link_markup(text)
    candidate = re.sub(r"[^A-Za-z\s'/-]", " ", candidate)
    words = re.findall(r"[A-Za-z]{4,}", candidate)
    return len(words) >= 2 or any(len(word) >= 8 for word in words)


def contains_untranslated_english_phrase(text: str) -> bool:
    candidate = strip_link_markup(text)
    candidate = re.sub(r"\((?:信息来源|引述自)[:：][^)]+\)", "", candidate)
    candidate = candidate.replace("[AI 推演]", "").replace("[AI推演]", "")
    english_runs = re.findall(r"\b[A-Za-z][A-Za-z0-9&'’.-]*(?:\s+[A-Za-z][A-Za-z0-9&'’.-]*){3,}\b", candidate)
    for run in english_runs:
        words = re.findall(r"[A-Za-z][A-Za-z0-9&'’.-]*", run)
        content_words = [word for word in words if word.upper() not in {"AI", "CEO", "CFO", "CTO", "US", "UK", "EU"}]
        if len(content_words) >= 4:
            return True
    return False


def has_forbidden_english_residue(text: str) -> bool:
    candidate = strip_link_markup(text)
    candidate = re.sub(r"\((?:信息来源|引述自)[:：][^)]+\)", "", candidate)
    candidate = candidate.replace("[AI 推演]", "").replace("[AI推演]", "")
    candidate = clean_text(candidate)
    return contains_substantial_english(candidate) and (
        not contains_cjk(candidate) or contains_untranslated_english_phrase(candidate)
    )


def normalize_source_article_title(title: str, url: str = "") -> Tuple[str, str]:
    value = clean_text(title)
    embedded_url_match = re.match(r"^\[+\s*(.+?)\s*\[(https?://[^\]]+)\]\s*\]+$", value)
    if embedded_url_match:
        value = embedded_url_match.group(1).strip()
        url = url or embedded_url_match.group(2).strip()

    value = re.sub(r"^\[+\s*", "", value)
    value = re.sub(r"\s*\]+$", "", value)
    return clean_text(value), clean_text(url)


def is_valid_traceability_source(source: str) -> bool:
    cleaned = clean_label_text(source)
    if not cleaned:
        return False
    if cleaned.startswith("[") or "://" in cleaned or cleaned.lower().startswith("http"):
        return False
    if re.match(r"^\d+\.\s+", cleaned):
        return False
    if re.search(r"[📰📌🔴🔵⚖️🔗⏱️]", cleaned):
        return False
    for term in ("全景综述", "核心事实", "新闻时间线", "视角", "客观共识", "风险推演", "AI 推演", "AI推演", "溯源印证", "机会", "风险"):
        if term in cleaned:
            return False
    return True


def extract_json_object(text: str) -> Dict[str, str]:
    candidate = text.strip()
    candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
    candidate = re.sub(r"\s*```$", "", candidate)
    match = re.search(r"\{.*\}", candidate, flags=re.DOTALL)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def translate_title_map(targets: Dict[str, str]) -> Dict[str, str]:
    if not targets:
        return {}

    prompt = f"""
你是中文终校编辑。请把下列英文新闻标题翻译成简体中文标题。

规则：
1. 只翻译标题文本本身，不要加编号，不要加引号，不要加解释。
2. 保持新闻标题风格，简洁、中性、准确。
3. 公司名、人名、组织名可以保留常见英文专名，但整体标题必须是中文。
4. 只返回 JSON 对象，键必须保持原样。

待翻译标题：
{json.dumps(targets, ensure_ascii=False, indent=2)}
"""
    try:
        return extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] 英文标题终校失败，保留原标题: {exc}")
        return {}


def translate_sentence_map(targets: Dict[str, str]) -> Dict[str, str]:
    if not targets:
        return {}

    prompt = f"""
你是中文终校编辑。请把下列英文新闻摘要翻译成简体中文的一句话概述。

规则：
1. 只翻译文本本身，不要加编号，不要加引号，不要加解释。
2. 保持新闻摘要风格，简洁、中性、准确。
3. 公司名、人名、组织名可以保留常见英文专名，但整体句子必须是中文。
4. 只返回 JSON 对象，键必须保持原样。

待翻译内容：
{json.dumps(targets, ensure_ascii=False, indent=2)}
"""
    try:
        return extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] 英文摘要终校失败，保留原文: {exc}")
        return {}


def iter_translation_batches(
    targets: Dict[str, str],
    max_items: int,
    max_chars: int,
) -> List[Dict[str, str]]:
    batches: List[Dict[str, str]] = []
    current: Dict[str, str] = {}
    current_chars = 0

    for key, value in targets.items():
        value_len = len(value)
        if current and (len(current) >= max_items or current_chars + value_len > max_chars):
            batches.append(current)
            current = {}
            current_chars = 0
        current[key] = value
        current_chars += value_len

    if current:
        batches.append(current)
    return batches


def translate_remaining_english_headings(md_text: str) -> str:
    lines = md_text.splitlines()
    targets: Dict[str, str] = {}
    positions: List[Tuple[int, str, str]] = []

    for index, raw in enumerate(lines):
        stripped = raw.strip()
        prefix_match = re.match(r"^(###\s+\d+\.\s+)(.*)$", stripped)
        if not prefix_match:
            continue
        title = clean_label_text(prefix_match.group(2))
        if not title or contains_cjk(title) or not contains_substantial_english(title):
            continue
        key = f"h{len(targets) + 1}"
        targets[key] = title
        positions.append((index, prefix_match.group(1), key))

    if not targets:
        return md_text

    batch_items = 16 if should_avoid_heavy_llm_repairs() else 12
    batch_chars = 2800 if should_avoid_heavy_llm_repairs() else 2200
    translated: Dict[str, str] = {}
    for batch in iter_translation_batches(targets, batch_items, batch_chars):
        translated.update(translate_title_map(batch))

    for index, prefix, key in positions:
        value = clean_label_text(str(translated.get(key, "")))
        if value and contains_cjk(value):
            lines[index] = f"{prefix}{value}"
    return "\n".join(lines)


def translate_remaining_english_fields(md_text: str) -> str:
    lines = md_text.splitlines()
    targets: Dict[str, str] = {}
    positions: List[Tuple[int, str, str, str]] = []

    for index, raw in enumerate(lines):
        stripped = raw.strip()
        prefix_match = re.match(r"^(- [📰⏱️📌🔴🔵⚖️] \*\*[^*]+\*\*：)(.*)$", stripped)
        if not prefix_match:
            continue
        prefix = prefix_match.group(1)
        body = prefix_match.group(2).strip()
        if not contains_substantial_english(body):
            continue

        suffix = ""
        quote_suffix_match = re.search(r"(\s*[\(（]\s*引述自[:：][^)）]+?报道?\s*[\)）]\s*)$", body)
        source_suffix_match = re.search(r"(\s*[\(（]\s*信息来源[:：][^)）]+[\)）]\s*)$", body)
        markdown_source_suffix_match = re.search(r"(\[来源[:：][^\]]+\]\(https?://[^)]+\))$", body)
        if quote_suffix_match:
            suffix = quote_suffix_match.group(1)
            body = body[:quote_suffix_match.start()].rstrip()
        elif source_suffix_match:
            suffix = source_suffix_match.group(1)
            body = body[:source_suffix_match.start()].rstrip()
        elif markdown_source_suffix_match:
            suffix = markdown_source_suffix_match.group(1)
            body = body[:markdown_source_suffix_match.start()].rstrip()

        if not body or not contains_substantial_english(body):
            continue

        key = f"f{len(targets) + 1}"
        targets[key] = body
        positions.append((index, prefix, suffix, key))

    if not targets:
        return md_text

    batch_items = 18 if should_avoid_heavy_llm_repairs() else 10
    batch_chars = 4000 if should_avoid_heavy_llm_repairs() else 2600
    translated: Dict[str, str] = {}
    for batch in iter_translation_batches(targets, batch_items, batch_chars):
        translated.update(translate_sentence_map(batch))

    for index, prefix, suffix, key in positions:
        value = clean_text(str(translated.get(key, "")))
        if value and contains_cjk(value):
            lines[index] = f"{prefix}{value}{suffix}"
    return "\n".join(lines)


def translate_remaining_quick_hits(md_text: str) -> str:
    lines = md_text.splitlines()
    title_targets: Dict[str, str] = {}
    summary_targets: Dict[str, str] = {}
    positions: List[Tuple[int, str, str, str, str]] = []
    in_quick_hits = False

    for index, raw in enumerate(lines):
        stripped = raw.strip()
        if stripped == QUICK_HITS_TITLE:
            in_quick_hits = True
            continue
        if in_quick_hits and stripped.startswith("## ") and stripped != QUICK_HITS_TITLE:
            break
        if not in_quick_hits or not stripped.startswith("* "):
            continue

        match = re.match(r"^(\*\s+(?:🚨\s+`?\[独家重磅\]`?\s+|[^\s]+\s+))\*\*(.+?)\*\*：(.*)$", stripped)
        if not match:
            continue

        prefix = match.group(1)
        title = clean_label_text(match.group(2))
        rest = match.group(3).strip()
        source_suffix = ""
        source_match = re.search(r"(（\[来源[:：].*）)$", rest)
        if source_match:
            source_suffix = source_match.group(1)
            rest = rest[:source_match.start()].rstrip()
        summary = clean_text(rest)

        title_key = f"qht{len(title_targets) + 1}"
        summary_key = f"qhs{len(summary_targets) + 1}"
        if title and has_forbidden_english_residue(title):
            title_targets[title_key] = title
        else:
            title_key = ""
        if summary and has_forbidden_english_residue(summary):
            summary_targets[summary_key] = summary
        else:
            summary_key = ""

        positions.append((index, prefix, title_key, summary_key, source_suffix))

    if not positions:
        return md_text

    translated_titles: Dict[str, str] = {}
    translated_summaries: Dict[str, str] = {}
    if title_targets:
        for batch in iter_translation_batches(title_targets, 12, 2200):
            translated_titles.update(translate_title_map(batch))
    if summary_targets:
        for batch in iter_translation_batches(summary_targets, 12, 2600):
            translated_summaries.update(translate_sentence_map(batch))

    for index, prefix, title_key, summary_key, source_suffix in positions:
        match = re.match(r"^(\*\s+(?:🚨\s+`?\[独家重磅\]`?\s+|[^\s]+\s+))\*\*(.+?)\*\*：(.*)$", lines[index].strip())
        if not match:
            continue
        title = clean_label_text(match.group(2))
        rest = match.group(3).strip()
        source_match = re.search(r"(（\[来源[:：].*）)$", rest)
        if source_match:
            rest = rest[:source_match.start()].rstrip()
        summary = clean_text(rest)

        translated_title = clean_label_text(str(translated_titles.get(title_key, title))) if title_key else title
        translated_summary = clean_text(str(translated_summaries.get(summary_key, summary))) if summary_key else summary
        if has_forbidden_english_residue(translated_summary) and contains_cjk(translated_title):
            translated_summary = translated_title
        translated_summary = ensure_terminal_punctuation(condense_summary_sentence(translated_summary or translated_title))
        suffix = f"{translated_summary}{source_suffix}" if source_suffix else translated_summary
        lines[index] = f"{prefix}**{translated_title}**：{suffix}"

    return "\n".join(lines)


def quick_hits_has_english_residue(md_text: str) -> bool:
    for raw in extract_markdown_section_lines(md_text, QUICK_HITS_TITLE):
        stripped = raw.strip()
        match = re.match(r"^\*\s+(?:🚨\s+`?\[独家重磅\]`?\s+|[^\s]+\s+)\*\*(.+?)\*\*：(.*)$", stripped)
        if not match:
            continue
        title = clean_label_text(match.group(1))
        summary = clean_text(re.sub(r"（\[来源[:：].*）\s*$", "", match.group(2)).strip())
        if has_forbidden_english_residue(title) or has_forbidden_english_residue(summary):
            return True
    return False


def deep_field_has_english_residue(stripped_line: str) -> bool:
    match = re.match(r"^\s*-\s+[📰⏱️📌🔴🔵⚖️]\s+\*\*[^*]+\*\*：(.*)$", stripped_line)
    if not match:
        return False
    body = clean_text(match.group(1))
    body = re.sub(r"\(信息来源: [^)]+\)$", "", body).strip()
    body = re.sub(r"\(引述自: [^)]+\)$", "", body).strip()
    return has_forbidden_english_residue(body)


def report_has_english_heading_or_field_residue(md_text: str) -> bool:
    for raw in md_text.splitlines():
        stripped = raw.strip()
        heading_match = re.match(r"^###\s+\d+\.\s+(.+)$", stripped)
        if heading_match and has_forbidden_english_residue(clean_label_text(heading_match.group(1))):
            return True

        if deep_field_has_english_residue(stripped):
            return True
    return False


def normalize_rendered_source_suffixes(md_text: str) -> str:
    normalized: List[str] = []
    for raw in md_text.splitlines():
        line = raw
        stripped = raw.strip()

        if stripped.startswith("- 📌 **核心事实**："):
            match = re.match(r"^(- 📌 \*\*核心事实\*\*：.*?)(?:\s*[\(（]?\s*(?:信息)?来源[:：]\s*([^)）]+)[)）]?)\s*$", stripped)
            if match:
                body = match.group(1).rstrip(" 。")
                source = clean_label_text(match.group(2))
                line = f"{body} (信息来源: {source})"

        if stripped.startswith("- 🔴 ") or stripped.startswith("- 🔵 "):
            match = re.match(r"^(.*?)(?:\s*[\(（]?\s*引述自[:：]\s*([^)）]+?)(?:\s*报道)?[)）]?)\s*$", stripped)
            if match:
                body = match.group(1).rstrip(" 。")
                source = clean_label_text(match.group(2))
                line = f"{body} (引述自: {source} 报道)"

        normalized.append(line)
    return "\n".join(normalized)


def get_candidate_primary_secondary_sources(candidate: Dict[str, object]) -> Tuple[str, str]:
    items = candidate.get("items", [])
    primary_source = canonicalize_source_name(str(items[0].get("source", ""))) if items else ""
    primary_source = primary_source or canonicalize_source_name(str(candidate.get("primary_source", ""))) or "来源"
    secondary_source = primary_source
    for item in items[1:]:
        source = canonicalize_source_name(str(item.get("source", "")))
        if source and source != primary_source:
            secondary_source = source
            break
    return primary_source, secondary_source


def ensure_deep_source_suffixes(md_text: str, selections: Dict[str, object]) -> str:
    section_key_map = {
        "## 🇨🇳【中国与世界 / China & The World】": "china_focus_candidates",
        "## 🌍【全球局势 / Global Affairs】": "global_affairs_candidates",
        "## 📈【商业与市场 / Business & Markets】": "business_market_candidates",
        "## 🚀【科技与AI / Tech & AI】": "tech_ai_candidates",
    }
    lines = md_text.splitlines()
    restored: List[str] = []
    current_section = ""
    current_candidate: Optional[Dict[str, object]] = None

    for raw in lines:
        stripped = raw.strip()
        if stripped in section_key_map:
            current_section = stripped
            current_candidate = None
            restored.append(raw)
            continue

        if stripped.startswith("## ") and stripped not in section_key_map:
            current_section = ""
            current_candidate = None
            restored.append(raw)
            continue

        heading_match = re.match(r"^###\s+(\d+)\.\s+", stripped)
        if heading_match and current_section:
            candidates = selections.get(section_key_map[current_section], [])
            candidate_index = int(heading_match.group(1)) - 1
            current_candidate = candidates[candidate_index] if 0 <= candidate_index < len(candidates) else None
            restored.append(raw)
            continue

        line = raw
        if current_candidate and stripped.startswith("- 📌 **核心事实**：") and "信息来源:" not in stripped:
            primary_source, _ = get_candidate_primary_secondary_sources(current_candidate)
            line = f"{raw.rstrip(' 。')} (信息来源: {primary_source})"
        elif current_candidate and stripped.startswith("- 🔴 ") and "引述自:" not in stripped and "基于:" not in stripped:
            primary_source, _ = get_candidate_primary_secondary_sources(current_candidate)
            line = f"{raw.rstrip(' 。')} (基于: {primary_source} 报道)"
        elif current_candidate and stripped.startswith("- 🔵 ") and "引述自:" not in stripped and "基于:" not in stripped:
            _, secondary_source = get_candidate_primary_secondary_sources(current_candidate)
            line = f"{raw.rstrip(' 。')} (基于: {secondary_source} 报道)"

        restored.append(line)
    return "\n".join(restored)


def polish_markdown_field_artifacts(md_text: str) -> str:
    polished: List[str] = []
    for raw in md_text.splitlines():
        line = raw
        stripped = raw.strip()
        if any(stripped.startswith(prefix) for prefix in ("- 📰 ", "- ⏱️ ", "- 📌 ", "- 🔴 ", "- 🔵 ", "- ⚖️ ")):
            line = re.sub(r"(?<!\[)\s*来源[:：]\s*[^\]]+\]\(https?://[^)]+\)\]?\s*$", "", line)
            line = re.sub(r"\[\s*。?\s*(\[[^\]]*来源[:：][^\]]+\]\(https?://[^)]+\))\s*\]", r"。\1", line)
            line = re.sub(r"(- 📌 \*\*核心事实\*\*：[^。\n\[]+)(\[[^\]]*来源[:：])", r"\1。\2", line)
            line = re.sub(r"(- 📌 \*\*核心事实\*\*：[^。\n]+)(?=来源[:：])", r"\1。", line)
        polished.append(line)
    return "\n".join(polished)


def normalize_deep_field_line(text: str) -> Optional[str]:
    label, body = split_label_and_body(text)
    normalized_label = clean_label_text(label)
    canonical_label = re.sub(r"^[📰⏱️📌🔗⚖️]+\s*", "", normalized_label).strip()

    field_map = {
        "全景综述": "📰",
        "新闻时间线": "⏱️",
        "核心事实": "📌",
        "溯源印证": "🔗",
    }

    if canonical_label in field_map:
        if canonical_label == "核心事实" and body:
            body = re.sub(r"(?<![。！？!?])(\[来源[:：])", r"。\1", body)
            body = re.sub(r"(?<![。！？!?\[])(\s*来源[:：])", r"。\1", body)
        suffix = f"：{body}" if body else "："
        return f"- {field_map[canonical_label]} **{canonical_label}**{suffix}"

    if canonical_label in {"[AI 推演]", "[AI推演]", "AI 推演", "AI推演", "客观共识与风险推演"}:
        body = re.sub(r"^\[?AI\s*推演\]?\s*[:：]?\s*", "", body).strip()
        suffix = f"：{body}" if body else "："
        return f"- ⚖️ **AI推演**{suffix}"

    if normalized_label.startswith("🔴") or ("视角" in normalized_label and "🔴" in text):
        clean_label = clean_label_text(normalized_label.removeprefix("🔴").strip())
        suffix = f"：{body}" if body else "："
        return f"- 🔴 **{clean_label}**{suffix}"

    if normalized_label.startswith("🔵") or ("视角" in normalized_label and "🔵" in text):
        clean_label = clean_label_text(normalized_label.removeprefix("🔵").strip())
        suffix = f"：{body}" if body else "："
        return f"- 🔵 **{clean_label}**{suffix}"

    return None


def consume_ai_lines(lines: List[str], start: int) -> Tuple[str, int]:
    upside_text = ""
    downside_text = ""
    index = start + 1
    while index < len(lines):
        raw = lines[index]
        stripped = raw.strip()
        if not stripped:
            index += 1
            continue

        label, body = split_label_and_body(stripped)
        label = strip_md_wrappers(label)
        if label not in {"机会", "优势", "风险", "劣势"}:
            break

        if label in {"机会", "优势"}:
            upside_text = body
        else:
            downside_text = body
        index += 1

    merged_line = "- ⚖️ **AI推演**："
    if upside_text and downside_text:
        merged_line += f"若{clean_text(upside_text).rstrip('。')}，事件会获得短期缓冲；但{clean_text(downside_text).rstrip('。')}一旦成为主导变量，局势将重新转向压力定价。"
    elif upside_text:
        merged_line += f"这一路径需要转化为可验证动作；若{clean_text(upside_text).rstrip('。')}，受力方会获得短期调整窗口。"
    elif downside_text:
        merged_line += f"压力若继续累积，局势会更快进入重新定价阶段；若{clean_text(downside_text).rstrip('。')}，最直接受力方会被迫提前调整政策或市场定价。"
    return merged_line, index


def parse_source_entry_text(text: str) -> Optional[Tuple[str, str, str]]:
    candidate = strip_list_prefix(text).strip()
    candidate = candidate.lstrip("*").strip()

    markdown_match = re.match(r"^\*{0,2}(.+?)\*{0,2}\s*:\s*\[([^\]]+)\]\((https?://[^)]+)\)\s*$", candidate)
    if markdown_match:
        source = clean_label_text(markdown_match.group(1))
        title, url = normalize_source_article_title(markdown_match.group(2).strip(), markdown_match.group(3).strip())
        if is_valid_traceability_source(source) and title:
            return source, title, url
        return None

    paren_match = re.match(r"^\*{0,2}(.+?)\*{0,2}\s*:\s*(.+?)\((https?://[^)]+)\)\s*$", candidate)
    if paren_match:
        source = clean_label_text(paren_match.group(1))
        title, url = normalize_source_article_title(paren_match.group(2).strip(), paren_match.group(3).strip())
        if is_valid_traceability_source(source) and title:
            return source, title, url
        return None

    plain_match = re.match(r"^\*{0,2}(.+?)\*{0,2}\s*:\s*(.+?)\s*$", candidate)
    if plain_match:
        source = clean_label_text(plain_match.group(1))
        title, url = normalize_source_article_title(plain_match.group(2).strip(), "")
        if is_valid_traceability_source(source) and title:
            return source, title, url
    return None


def split_source_entry_chunks(text: str) -> List[str]:
    candidate = text.strip()
    if not candidate:
        return []
    candidate = re.sub(r"^\*\s*", "", candidate)
    parts = re.split(r"\s+\*\s+(?=\*{0,2}.+?\*{0,2}\s*:)", candidate)
    return [part.strip() for part in parts if part.strip()]


def lookup_source_url(source: str, title: str, source_catalog: Sequence[Dict[str, object]]) -> str:
    normalized_source = clean_label_text(source)
    normalized_title = clean_text(title)
    for cluster in source_catalog:
        for item in cluster.get("items", []):
            item_source = clean_label_text(str(item.get("source", "")))
            item_title = clean_text(str(item.get("title", "")))
            item_link = clean_text(str(item.get("link", "")))
            if not item_title or not item_link:
                continue
            if normalized_source and normalized_source != item_source:
                continue
            if titles_match(normalized_title, item_title) or normalized_title in item_title or item_title in normalized_title:
                return item_link
    return ""


def consume_source_lines(
    lines: List[str],
    start: int,
    initial_text: str = "",
    source_catalog: Sequence[Dict[str, object]] = (),
) -> Tuple[List[str], int]:
    pairs: List[Tuple[str, str]] = []
    pending_title = ""
    index = start + 1

    for chunk in split_source_entry_chunks(initial_text):
        initial_entry = parse_source_entry_text(chunk)
        if not initial_entry:
            continue
        source, title, url = initial_entry
        url = url or lookup_source_url(source, title, source_catalog)
        link_md = f"[{title}]({url})" if url else title
        pairs.append((source, link_md))

    while index < len(lines):
        stripped = lines[index].strip()
        if not stripped:
            index += 1
            continue

        direct_chunks = split_source_entry_chunks(stripped)
        if direct_chunks:
            parsed_any = False
            for chunk in direct_chunks:
                direct_entry = parse_source_entry_text(chunk)
                if not direct_entry:
                    continue
                source, title, url = direct_entry
                url = url or lookup_source_url(source, title, source_catalog)
                link_md = f"[{title}]({url})" if url else title
                pairs.append((source, link_md))
                parsed_any = True
            if parsed_any:
                index += 1
                continue

        label, body = split_label_and_body(stripped)
        normalized_label = clean_label_text(label)

        if normalized_label in {"标题", "原文标题", "外媒原报道真实标题"}:
            pending_title = body
            index += 1
            continue
        if normalized_label == "链接" and pending_title:
            pairs.append((infer_source_name_from_url(body), f"[{pending_title}]({body})"))
            pending_title = ""
            index += 1
            continue
        break

    if not pairs:
        return ["- 🔗 **溯源印证**："], start + 1

    normalized = ["- 🔗 **溯源印证**："]
    for source, link_md in pairs:
        normalized.append(f"    * **{source}**: {link_md}")
    return normalized, index


def choose_quick_hit_emoji(text: str) -> str:
    lower = text.lower()
    if re.search(r"\b(court|judge|ruling|law|policy|congress|parliament|supreme court|regulator)\b", lower) or re.search(r"(法院|法官|裁决|法案|政策|监管|议会|国会)", text):
        return "🏛️"
    if re.search(r"\b(openai|anthropic|deepmind|xai|ai|robot|chip|chips|semiconductor|nvidia|intel|amazon|google|microsoft)\b", lower) or re.search(r"(人工智能|机器人|芯片|半导体|算力|模型|OpenAI|谷歌|微软|亚马逊)", text):
        return "🤖"
    if re.search(r"\b(oil|gas|energy|hormuz|pipeline|refiner|refinery|coal)\b", lower) or re.search(r"(能源|原油|天然气|煤炭|霍尔木兹|炼油|油价)", text):
        return "⚡"
    if re.search(r"\b(rate|inflation|stocks?|bonds?|market|economy|tariff|trade|currency|debt|bank)\b", lower) or re.search(r"(利率|通胀|市场|经济|关税|贸易|债券|汇率|银行)", text):
        return "📈"
    if re.search(r"\b(measles|health|disease|vaccine|hospital)\b", lower) or re.search(r"(麻疹|疫苗|疫情|疾病|医院|卫生)", text):
        return "🩺"
    if re.search(r"\b(iran|russia|ukraine|israel|gaza|military|rescue|war|strike|sanction|election|diplom)\b", lower) or re.search(r"(伊朗|俄罗斯|乌克兰|以色列|加沙|军事|营救|冲突|制裁|空袭|外交|选举)", text):
        return "🌍"
    return "📰"


def strip_leading_decorations(text: str) -> str:
    return LEADING_DECORATION_RE.sub("", text).strip()


def extract_quick_hit_sources(text: str) -> List[str]:
    linked_sources = re.findall(r"\[来源[:：]\s*([^\]]+)\]\(", text)
    if linked_sources:
        return [canonicalize_source_name(source) for source in linked_sources]

    fallback_sources = re.findall(r"来源[:：]\s*([^\],，)]+)", text)
    return [canonicalize_source_name(source) for source in fallback_sources]


def quick_hit_exclusive_is_valid(text: str) -> bool:
    cited_sources = list(dict.fromkeys(extract_quick_hit_sources(text)))
    if len(cited_sources) != 1:
        return False
    if not is_tier1_source(cited_sources[0]):
        return False

    lower = clean_text(text).lower()
    if any(pattern.search(lower) for pattern in MAJOR_EXCLUSIVE_VETO_PATTERNS):
        return False
    if not any(pattern.search(lower) for pattern, _ in MAJOR_EXCLUSIVE_SYSTEMIC_PATTERNS):
        return False
    return True


def normalize_quick_hit_line(text: str) -> Optional[str]:
    body = strip_list_prefix(text)
    body = strip_leading_decorations(body)
    is_exclusive = "独家重磅" in body
    source_count = len(set(extract_quick_hit_sources(body)))

    if is_exclusive:
        if not quick_hit_exclusive_is_valid(body):
            return None
        rest = re.sub(r"`?\[独家重磅\]`?", "", body).strip()
        rest = strip_leading_decorations(rest)
        return f"* 🚨 `[独家重磅]` {rest}" if rest else None

    if source_count < 2:
        return None

    cleaned = strip_leading_decorations(body)
    emoji = choose_quick_hit_emoji(cleaned)
    return f"* {emoji} {cleaned}" if cleaned else None


def normalize_quick_hits_entries(lines: List[str]) -> List[str]:
    regular: List[str] = []
    exclusives: List[str] = []

    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        if not re.match(r"^(?:[-*]|\d+\.)\s+", stripped):
            continue

        normalized_line = normalize_quick_hit_line(stripped)
        if not normalized_line:
            continue
        if normalized_line.startswith("* 🚨"):
            exclusives.append(normalized_line)
        else:
            regular.append(normalized_line)

    exclusives = exclusives[:QUICK_HITS_MAX_EXCLUSIVE]
    regular_cap = max(0, QUICK_HITS_MAX_TOTAL - len(exclusives))
    return (regular[:regular_cap] + exclusives)[:QUICK_HITS_MAX_TOTAL]


def ensure_terminal_punctuation(text: str) -> str:
    value = clean_text(text).rstrip("，,；;：:")
    if not value:
        return ""
    if re.search(r"[。！？!?]$", value):
        return value
    return f"{value}。"


def condense_summary_sentence(text: str, limit: int = 82) -> str:
    value = clean_text(text)
    if not value:
        return ""
    parts = re.split(r"(?<=[。！？!?;；])\s+", value)
    if parts and parts[0]:
        value = parts[0].strip()
    value = value.rstrip("，,；;：:")
    if len(value) > limit:
        truncated = value[:limit].rstrip("，,；;：: ")
        if (
            re.search(r"[A-Za-z0-9]$", truncated)
            and limit < len(value)
            and re.match(r"[A-Za-z0-9]", value[limit:limit + 1] or "")
        ):
            trimmed = re.sub(r"[A-Za-z0-9_-]+$", "", truncated).rstrip("，,；;：: ")
            if len(trimmed) >= max(16, limit // 2):
                truncated = trimmed
        value = truncated
    return value


def pick_quick_hit_summary(candidate: Dict[str, object]) -> str:
    headline = clean_text(candidate.get("headline", ""))
    for item in candidate.get("items", []):
        summary = clean_text(item.get("summary", ""))
        if summary and (not headline or not titles_match(summary, headline)):
            return condense_summary_sentence(summary)

    for item in candidate.get("items", [])[1:]:
        alt_title = clean_text(item.get("title", ""))
        if alt_title and (not headline or not titles_match(alt_title, headline)):
            return condense_summary_sentence(alt_title)

    return condense_summary_sentence(headline)


def collect_candidate_source_refs(candidate: Dict[str, object], limit: int) -> List[Tuple[str, str]]:
    refs: List[Tuple[str, str]] = []
    seen = set()
    for item in candidate.get("items", []):
        source = canonicalize_source_name(str(item.get("source", "")))
        url = clean_text(item.get("link", ""))
        if not source or not url or source in seen:
            continue
        refs.append((source, url))
        seen.add(source)
        if len(refs) >= limit:
            break
    return refs


def render_quick_hit_source_refs(refs: Sequence[Tuple[str, str]]) -> str:
    if not refs:
        return ""
    joined = ", ".join(f"[来源: {source}]({url})" for source, url in refs if source and url)
    return f"（{joined}）" if joined else ""


def build_quick_hits_fallback_candidates(selections: Dict[str, object]) -> List[Dict[str, object]]:
    combined: List[Dict[str, object]] = []
    for key in (
        "quick_hits_candidates",
        "china_focus_candidates",
        "global_affairs_candidates",
        "business_market_candidates",
        "tech_ai_candidates",
    ):
        for candidate in selections.get(key, []):
            if any(candidates_match(candidate, existing) for existing in combined):
                continue
            combined.append({**candidate, "quick_hits_mode": "fallback"})
    return rank_candidates(combined, "quick_hits_score")


def candidate_identity(candidate: Dict[str, object]) -> str:
    headline = clean_text(candidate.get("headline", ""))
    if headline:
        return headline
    for item in candidate.get("items", []):
        link = clean_text(item.get("link", ""))
        if link:
            return link
    return ""


def build_quick_hits_lines_from_candidates(selections: Dict[str, object]) -> List[str]:
    entries: List[Dict[str, object]] = []
    quick_hits_candidates = list(selections.get("quick_hits_candidates", []))[:QUICK_HITS_MAX_TOTAL]
    if not quick_hits_candidates:
        quick_hits_candidates = build_quick_hits_fallback_candidates(selections)[:SECTION_DEFAULT_ITEMS]

    for idx, candidate in enumerate(quick_hits_candidates, start=1):
        title = clean_text(candidate.get("headline", ""))
        summary = pick_quick_hit_summary(candidate)

        entries.append(
            {
                "candidate": candidate,
                "title": title,
                "summary": summary,
                "mode": candidate.get("quick_hits_mode", "regular"),
            }
        )
    lines: List[str] = []

    for entry in entries:
        title = clean_label_text(str(entry["title"]))
        summary = clean_text(str(entry["summary"]))
        summary = ensure_terminal_punctuation(condense_summary_sentence(summary or title))
        mode = str(entry["mode"])
        refs = collect_candidate_source_refs(entry["candidate"], 1 if mode in {"exclusive", "fallback"} else 2)
        if mode == "regular" and len(refs) < 2:
            mode = "fallback"
            refs = collect_candidate_source_refs(entry["candidate"], 1)
        source_refs = render_quick_hit_source_refs(refs)
        suffix = f"{summary}{source_refs}" if source_refs else summary

        if mode == "exclusive":
            if not refs:
                continue
            lines.append(f"* 🚨 `[独家重磅]` **{title}**：{suffix}")
            continue

        if mode != "fallback" and len(refs) < 2:
            continue
        emoji = choose_quick_hit_emoji(f"{title} {summary}")
        lines.append(f"* {emoji} **{title}**：{suffix}")

    if not lines:
        fallback_candidates = build_quick_hits_fallback_candidates(selections)[:SECTION_DEFAULT_ITEMS]
        if fallback_candidates != quick_hits_candidates:
            fallback_selections = dict(selections)
            fallback_selections["quick_hits_candidates"] = fallback_candidates
            return build_quick_hits_lines_from_candidates(fallback_selections)

    return lines[:QUICK_HITS_MAX_TOTAL]


def validate_editorial_selections(selections: Dict[str, object]) -> None:
    section_score_map = {
        "china_focus_candidates": "section_scores.china",
        "global_affairs_candidates": "section_scores.global",
        "business_market_candidates": "section_scores.business",
        "tech_ai_candidates": "section_scores.tech",
    }
    section_pool_counts = selections.get("section_pool_counts", {})
    section_candidate_pools = selections.get("section_candidate_pools", {})

    all_selected: List[Tuple[str, Dict[str, object]]] = []
    for section_key in section_score_map:
        for candidate in selections.get(section_key, []):
            all_selected.append((section_key, candidate))

    seen_candidates: List[Tuple[str, Dict[str, object]]] = []
    for section_key, score_key in section_score_map.items():
        candidates = list(selections.get(section_key, []))
        fillable_candidates = list(candidates)
        for pool_candidate in section_candidate_pools.get(section_key, []):
            if any(candidates_match(pool_candidate, existing) for existing in fillable_candidates):
                continue
            if any(
                other_section != section_key and candidates_match(pool_candidate, other_candidate)
                for other_section, other_candidate in all_selected
            ):
                continue
            fillable_candidates.append(pool_candidate)

        available_count = len(fillable_candidates)
        minimum_required = min(SECTION_MIN_ITEMS, min(available_count, int(section_pool_counts.get(section_key, available_count))))
        if len(candidates) < minimum_required:
            raise RuntimeError(f"{section_key} 候选不足: 期望至少 {minimum_required}，实际 {len(candidates)}")
        for index, candidate in enumerate(candidates, start=1):
            for previous_section, previous_candidate in seen_candidates:
                if previous_section != section_key and candidates_match(candidate, previous_candidate):
                    raise RuntimeError(
                        f"深读板块重复选中了同一新闻: "
                        f"{candidate_identity(candidate) or candidate.get('headline', '')} ({previous_section} / {section_key})"
                    )
            seen_candidates.append((section_key, candidate))

            if index <= SECTION_DEFAULT_ITEMS:
                continue

            llm_score = get_llm_importance_score(candidate)
            section_score = get_score_value(candidate, score_key).get("total_score", 0)
            if index == SECTION_DEFAULT_ITEMS + 1:
                allowed = llm_score >= SECTION_SOFT_EXTRA_LLM_SCORE or section_score >= SECTION_EXCEPTIONAL_SCORE - 1
            else:
                allowed = llm_score >= SECTION_EXCEPTIONAL_LLM_SCORE or section_score >= SECTION_EXCEPTIONAL_SCORE
            if not allowed:
                raise RuntimeError(
                    f"{section_key} 超出了默认 3 条，但额外条目分数不足: {identity or index}"
                )

    quick_hits_lines = build_quick_hits_lines_from_candidates(selections)
    fallback_candidates = build_quick_hits_fallback_candidates(selections)
    if fallback_candidates and not quick_hits_lines:
        raise RuntimeError("Quick Hits 候选存在，但最终无法生成条目。")


def validate_rendered_report(md_text: str, selections: Dict[str, object]) -> None:
    quick_hits_expected = build_quick_hits_lines_from_candidates(selections)
    quick_hits_block = extract_markdown_section_lines(md_text, QUICK_HITS_TITLE)
    quick_hits_bullets = [
        line.strip()
        for line in quick_hits_block
        if re.match(r"^\*\s+", line.strip())
    ]
    if quick_hits_expected and not quick_hits_bullets:
        raise RuntimeError("Quick Hits 在最终稿中缺失。")
    for bullet in quick_hits_bullets:
        match = re.match(r"^\*\s+(?:🚨\s+`?\[独家重磅\]`?\s+|[^\s]+\s+)\*\*(.+?)\*\*：(.*)$", bullet)
        if match:
            title = clean_label_text(match.group(1))
            summary = clean_text(re.sub(r"（\[来源[:：].*）\s*$", "", match.group(2)).strip())
            if has_forbidden_english_residue(title) or has_forbidden_english_residue(summary):
                raise RuntimeError(f"Quick Hits 存在未中文化条目: {bullet}")
        if "来源: news.google.com" in bullet:
            raise RuntimeError(f"Quick Hits 来源识别退化: {bullet}")

    section_key_map = {
        "## 🇨🇳【中国与世界 / China & The World】": "china_focus_candidates",
        "## 🌍【全球局势 / Global Affairs】": "global_affairs_candidates",
        "## 📈【商业与市场 / Business & Markets】": "business_market_candidates",
        "## 🚀【科技与AI / Tech & AI】": "tech_ai_candidates",
    }
    section_pool_counts = selections.get("section_pool_counts", {})

    rendered_seen: Dict[str, str] = {}
    for section_title, candidate_key in section_key_map.items():
        block = extract_markdown_section_lines(md_text, section_title)
        headings = []
        for line in block:
            match = re.match(r"^\s*###\s+\d+\.\s+(.+?)\s*$", line)
            if match:
                headings.append(clean_label_text(match.group(1)))

        expected_count = len(selections.get(candidate_key, []))
        minimum_required = min(SECTION_MIN_ITEMS, int(section_pool_counts.get(candidate_key, expected_count)))
        if expected_count != len(headings):
            raise RuntimeError(f"{section_title} 条数异常: 期望 {expected_count}，实际 {len(headings)}")
        if len(headings) < minimum_required:
            raise RuntimeError(f"{section_title} 少于最低条数: 期望至少 {minimum_required}，实际 {len(headings)}")

        for heading in headings:
            previous = rendered_seen.get(heading)
            if previous and previous != section_title:
                raise RuntimeError(f"最终稿跨板块重复标题: {heading} ({previous} / {section_title})")
            rendered_seen[heading] = section_title
            if has_forbidden_english_residue(heading):
                raise RuntimeError(f"{section_title} 标题未完成中文化: {heading}")
            if re.match(r"^(?:消息人士|知情人士|据悉|据报道|报道称|来源称)[:：]", heading):
                raise RuntimeError(f"{section_title} 标题仍带消息来源壳: {heading}")
        for idx, heading in enumerate(headings):
            for other in headings[idx + 1:]:
                if titles_match(heading, other):
                    raise RuntimeError(f"{section_title} 板块内部存在重复新闻: {heading} / {other}")

        ai_lines = [
            line.strip()
            for line in block
            if re.match(r"^\s*-\s+⚖️\s+\*\*(?:AI推演|客观共识与风险推演)\*\*：", line.strip())
        ]
        if len(ai_lines) != len(headings):
            raise RuntimeError(f"{section_title} AI推演条数异常: 期望 {len(headings)}，实际 {len(ai_lines)}")
        for ai_line in ai_lines:
            if "客观共识与风险推演" in ai_line:
                raise RuntimeError(f"{section_title} AI推演字段名未更新: {ai_line}")
            banned_ai_shapes = ("优势（机会）", "劣势（风险）", "机会端", "风险端", "一方面", "另一方面", "既有机遇也有挑战", "关键变量是")
            if any(shape in ai_line for shape in banned_ai_shapes):
                raise RuntimeError(f"{section_title} AI推演仍是标签化对仗结构: {ai_line}")
            if len(clean_text(ai_line)) < 45:
                raise RuntimeError(f"{section_title} AI推演内容不完整: {ai_line}")

        for line in block:
            stripped = line.strip()
            if deep_field_has_english_residue(stripped):
                raise RuntimeError(f"{section_title} 字段未完成中文化: {stripped}")
            if "信息来源: news.google.com" in stripped or "引述自: news.google.com" in stripped or "基于: news.google.com" in stripped or "**news.google.com**" in stripped:
                raise RuntimeError(f"{section_title} 来源识别退化: {stripped}")
            if stripped.startswith("- 📌 **核心事实**：") and "信息来源:" not in stripped:
                raise RuntimeError(f"{section_title} 核心事实来源格式异常: {stripped}")
            if stripped.startswith("- 📰 **全景综述**："):
                banned_overview_shapes = ("这条新闻的重要性在于", "这条新闻重要在于", "这条新闻的价值在于", "关键变量是")
                if any(shape in stripped for shape in banned_overview_shapes):
                    raise RuntimeError(f"{section_title} 全景综述仍是点评模板: {stripped}")
                if "](http" not in stripped:
                    raise RuntimeError(f"{section_title} 全景综述缺少可点击出处 URL: {stripped}")
            if stripped.startswith("- 🔴 ") or stripped.startswith("- 🔵 "):
                perspective_label = re.match(r"^-\s+[🔴🔵]\s+\*\*([^*]+)\*\*：", stripped)
                if perspective_label:
                    label = clean_label_text(perspective_label.group(1))
                    media_label_pattern = r"^(Reuters|Bloomberg|Financial Times|FT|WSJ|AP|Associated Press|Al Jazeera|BBC|BBC World News|AFP|Nikkei Asia|South China Morning Post|Techmeme|TechCrunch).*(视角|观点)$"
                    if re.match(media_label_pattern, label, flags=re.IGNORECASE):
                        raise RuntimeError(f"{section_title} 视角标签仍是媒体视角: {stripped}")
                if "](http" not in stripped:
                    raise RuntimeError(f"{section_title} 视角字段缺少可点击出处 URL: {stripped}")
            if stripped.startswith("- ⚖️ **AI推演**：") and "](http" not in stripped:
                raise RuntimeError(f"{section_title} AI推演缺少可点击出处 URL: {stripped}")


def extract_markdown_section_lines(md_text: str, section_title: str) -> List[str]:
    lines = md_text.splitlines()
    index = 0
    while index < len(lines):
        if lines[index].strip() != section_title:
            index += 1
            continue
        index += 1
        block: List[str] = []
        while index < len(lines):
            candidate = lines[index].strip()
            if candidate.startswith("## ") and candidate != section_title:
                break
            block.append(lines[index])
            index += 1
        return block
    return []


def replace_markdown_section(md_text: str, section_title: str, body_lines: Sequence[str], insert_before: Sequence[str] = ()) -> str:
    lines = md_text.splitlines()
    rebuilt: List[str] = []
    index = 0
    replaced = False
    inserted = False

    while index < len(lines):
        stripped = lines[index].strip()
        if stripped == section_title:
            rebuilt.append(section_title)
            rebuilt.append("")
            rebuilt.extend(body_lines)
            rebuilt.append("")
            replaced = True
            index += 1
            while index < len(lines):
                candidate = lines[index].strip()
                if candidate.startswith("## ") and candidate != section_title:
                    break
                index += 1
            continue

        if not replaced and not inserted and stripped in insert_before:
            rebuilt.append(section_title)
            rebuilt.append("")
            rebuilt.extend(body_lines)
            rebuilt.append("")
            inserted = True

        rebuilt.append(lines[index])
        index += 1

    if not replaced and not inserted:
        if rebuilt and rebuilt[-1].strip():
            rebuilt.append("")
        rebuilt.append(section_title)
        rebuilt.append("")
        rebuilt.extend(body_lines)

    cleaned: List[str] = []
    previous_blank = False
    for line in rebuilt:
        is_blank = not line.strip()
        if is_blank and previous_blank:
            continue
        cleaned.append(line)
        previous_blank = is_blank
    return "\n".join(cleaned).strip()


def remove_markdown_section(md_text: str, section_title: str) -> str:
    lines = md_text.splitlines()
    rebuilt: List[str] = []
    index = 0
    while index < len(lines):
        if lines[index].strip() == section_title:
            index += 1
            while index < len(lines):
                candidate = lines[index].strip()
                if candidate.startswith("## ") and candidate != section_title:
                    break
                index += 1
            continue
        rebuilt.append(lines[index])
        index += 1
    return "\n".join(rebuilt).strip()


def ensure_quick_hits_section(md_text: str, selections: Dict[str, object]) -> str:
    lines = build_quick_hits_lines_from_candidates(selections)
    return replace_markdown_section(md_text, QUICK_HITS_TITLE, lines, insert_before=tuple(DEEP_SECTION_TITLES))


def report_needs_structural_repair(md_text: str, selections: Dict[str, object]) -> bool:
    quick_hits_block = extract_markdown_section_lines(md_text, QUICK_HITS_TITLE)
    quick_hits_lines = normalize_quick_hits_entries(quick_hits_block)
    if selections.get("quick_hits_candidates") and not quick_hits_lines:
        return True

    section_key_map = {
        "## 🇨🇳【中国与世界 / China & The World】": "china_focus_candidates",
        "## 🌍【全球局势 / Global Affairs】": "global_affairs_candidates",
        "## 📈【商业与市场 / Business & Markets】": "business_market_candidates",
        "## 🚀【科技与AI / Tech & AI】": "tech_ai_candidates",
    }

    for section_title, candidate_key in section_key_map.items():
        block = extract_markdown_section_lines(md_text, section_title)
        headings = sum(1 for line in block if re.match(r"^\s*###\s+\d+\.\s+", line))
        fields = sum(1 for line in block if re.match(r"^\s*-\s+[📰⏱️📌🔴🔵⚖️🔗]\s+", line))
        minimum_expected = min(SECTION_MIN_ITEMS, len(selections.get(candidate_key, [])))
        if minimum_expected and headings < minimum_expected:
            return True
        if headings and fields < headings * 3:
            return True
    return False


def repair_report_structure(md_text: str, history: str, selections: Dict[str, object]) -> str:
    payload = {
        "history": history,
        "quick_hits_candidates": selections.get("quick_hits_candidates", []),
        "china_focus_candidates": selections.get("china_focus_candidates", []),
        "global_affairs_candidates": selections.get("global_affairs_candidates", []),
        "business_market_candidates": selections.get("business_market_candidates", []),
        "tech_ai_candidates": selections.get("tech_ai_candidates", []),
    }

    prompt = f"""
你是新闻简报的终校总编。下面这份草稿已经有事实基础，但格式和板块分配不稳定。请把它重写成严格合规的最终 Markdown。

硬性规则：
1. 只输出 Markdown，不要解释，不要代码块，不要 HTML。
2. 全文必须是简体中文；只有“溯源印证”里的外媒原标题可以保留原文英文。
3. 顶层结构顺序必须固定：
   历史上的今天
   ---
   ## 【Quick Hits】
   ## 🇨🇳【中国与世界 / China & The World】
   ## 🌍【全球局势 / Global Affairs】
   ## 📈【商业与市场 / Business & Markets】
   ## 🚀【科技与AI / Tech & AI】
   ---
   🎵 今日回响
4. Quick Hits 只允许无序列表 `*`，常规项格式必须是：
   * [1个合适Emoji] **中文标题**：中文一句话。[[来源: 媒体A](URL), [来源: 媒体B](URL)]
   独家项格式必须是：
   * 🚨 `[独家重磅]` **中文标题**：中文一句话。[[来源: 媒体](URL)]
5. 四个深读板块每条都必须严格使用以下骨架：
   ### [序号]. [中文标题]
   - 📰 **全景综述**：直接概括原始报道本身，写清谁做了什么、发生了什么、报道给出的关键背景和直接后果，80-130 字；禁止写“这条新闻的重要性在于”“关键变量是”等点评分析句式；句末必须以可点击链接标注主要出处。
   - ⏱️ **新闻时间线**：...  （仅在新闻事件本身有两个以上明确时间节点时输出；不要把媒体发布时间写成时间线）
   - 📌 **核心事实**：只写“发生了什么”，即具体动作、关键数字、当事方、时间节点；每条事实句末括注信息来源媒体名，禁止背景介绍、意义阐释或分析性语言。
   - 🔴 **[一方观点 / What one side is saying]**：像 Tangle 一样概括这一方最强论点，写清具名阵营、利益相关方、政策派别或市场群体为何这样看；句末必须以可点击链接标注该观点出处。
   - 🔵 **[另一方观点 / What the other side is saying]**：像 Tangle 一样概括另一方、反对方、受影响方或市场反馈的最强论点；没有左右派时，也要找出真实利益冲突或政策取舍；句末必须以可点击链接标注该观点出处。
   - ⚖️ **AI推演**：模仿 Tangle 的 “My take”：先公平承认两方各自说对了什么，再给出清晰编辑判断，60-100 字；禁止“优势/风险”“一方面/另一方面”等对仗标签；句末必须以可点击链接标注判断所依据的来源。
   - 🔗 **溯源印证**：
       * **媒体A**: [外媒原标题](URL)
       * **媒体B**: [外媒原标题](URL)
6. 四个深读板块默认最少 3 条，最多 6 条；只有候选确实不足时才少于 3 条，但绝对禁止输出“暂无”“无动态”等兜底废话。
7. 涉华板块只收录中国实体直接、显性涉及的事件；中国只是背景因素的新闻绝对不能放进去。
8. 不要让同一条新闻跨板块重复。
9. 历史上的今天保持一段轻量文字，不要 blockquote，不要 HTML。

当前草稿：
{md_text}

可用候选池（JSON）：
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
    try:
        return call_llm(prompt)
    except Exception as exc:
        print(f"[WARN] 结构终校失败，保留现有草稿: {exc}")
        return md_text


def format_candidate_published_time(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    try:
        published = datetime.fromisoformat(raw)
    except ValueError:
        return ""
    if published.tzinfo:
        published = published.astimezone(get_local_now().tzinfo)
    return f"{published.year}年{published.month}月{published.day}日 {published.hour:02d}:{published.minute:02d}"


def collect_candidate_report_fragments(candidate: Dict[str, object]) -> List[Dict[str, str]]:
    headline = clean_text(candidate.get("headline", ""))
    fragments: List[Dict[str, str]] = []

    for item in candidate.get("items", []):
        source = canonicalize_source_name(str(item.get("source", "")))
        title = clean_text(item.get("title", ""))
        title_text = condense_summary_sentence(title, limit=120)
        summary = condense_summary_sentence(item.get("summary", ""), limit=120)
        summary_matches_story = bool(
            summary
            and (
                not title_text
                or titles_match(summary, title_text)
                or match_similarity(summary, title_text) >= 0.42
                or match_similarity(summary, headline) >= 0.42
            )
        )
        if summary and contains_cjk(summary):
            text = summary
        else:
            text = summary if summary_matches_story else title_text
        if not text:
            continue
        if any(titles_match(text, fragment["text"]) for fragment in fragments):
            continue
        fragments.append(
            {
                "source": source or "相关媒体",
                "title": title,
                "text": text,
                "published": format_candidate_published_time(str(item.get("published", ""))),
            }
        )
        if len(fragments) >= 3:
            break

    if not fragments and headline:
        fragments.append(
            {
                "source": canonicalize_source_name(str(candidate.get("primary_source", ""))) or "相关媒体",
                "title": headline,
                "text": condense_summary_sentence(headline, limit=120),
                "published": "",
            }
        )
    return fragments


def build_candidate_corpus(candidate: Dict[str, object], fragments: Sequence[Dict[str, str]]) -> str:
    return clean_text(
        " ".join(
            [str(candidate.get("headline", ""))]
            + [fragment.get("title", "") for fragment in fragments]
            + [fragment.get("text", "") for fragment in fragments]
        )
    )


def corpus_has(corpus: str, pattern: str) -> bool:
    return bool(re.search(pattern, corpus, re.IGNORECASE))


def strip_report_scaffold(text: str, source: str = "") -> str:
    value = clean_text(text)
    value = re.sub(r"\s*[\(（]\s*信息来源[:：][^)）]+[)）]\s*$", "", value)
    source_tokens = [clean_label_text(source)]
    source_aliases = {
        "Al Jazeera": ["半岛电视台"],
        "BBC World News": ["BBC", "英国广播公司"],
        "Associated Press": ["AP", "美联社"],
        "Reuters": ["路透社"],
        "Bloomberg": ["彭博社"],
        "Financial Times": ["FT", "金融时报"],
        "WSJ": ["华尔街日报"],
    }
    source_tokens.extend(source_aliases.get(clean_label_text(source), []))
    source_tokens.extend(["Reuters", "Bloomberg", "Associated Press", "AP", "Al Jazeera", "BBC", "WSJ", "Financial Times"])
    for token in unique_preserving_order([item for item in source_tokens if item]):
        value = re.sub(
            rf"^{re.escape(token)}(?:报道|报道称|称|表示|指出|补充称)?[:：，,]\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )
    value = re.sub(r"^核心事实是[:：]\s*", "", value)
    value = value.strip(" ，,。；;：:")
    return value


def build_section_overview_text(
    candidate: Dict[str, object],
    fragments: Sequence[Dict[str, str]],
    section_title: str = "",
) -> str:
    summary_parts: List[str] = []
    for fragment in fragments[:2]:
        source = canonicalize_source_name(fragment.get("source", "")) or "相关媒体"
        text = strip_report_scaffold(fragment.get("text", "") or fragment.get("title", ""), source)
        if not text:
            continue
        text = condense_summary_sentence(text, limit=110).rstrip("。！？!?")
        if any(titles_match(text, existing) or text in existing or existing in text for existing in summary_parts):
            continue
        summary_parts.append(f"{source}报道，{text}")

    if summary_parts:
        return ensure_terminal_punctuation("；".join(summary_parts))
    return ensure_terminal_punctuation(condense_summary_sentence(candidate.get("headline", ""), limit=120))


EVENT_DATE_RE = re.compile(
    r"("
    r"20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}日?"
    r"|\d{1,2}月\d{1,2}日"
    r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s*20\d{2})?"
    r")",
    re.IGNORECASE,
)


def extract_explicit_event_date(text: str) -> str:
    value = clean_text(text)
    match = EVENT_DATE_RE.search(value)
    return match.group(1) if match else ""


def build_section_timeline_text(fragments: Sequence[Dict[str, str]]) -> str:
    events: List[str] = []
    seen = set()
    for fragment in fragments:
        combined_text = clean_text(" ".join([fragment.get("title", ""), fragment.get("text", "")]))
        event_date = extract_explicit_event_date(combined_text)
        if not event_date:
            continue
        if event_date in seen:
            continue
        event_text = condense_summary_sentence(fragment.get("text", "") or fragment.get("title", ""), limit=72)
        if not event_text:
            continue
        events.append(f"**[{event_date}]** {event_text}")
        seen.add(event_date)
        if len(events) >= 3:
            break
    if len(events) < 2:
        return ""
    return " ➡️ ".join(events)


def build_core_fact_text(candidate: Dict[str, object], fragments: Sequence[Dict[str, str]], timeline: str) -> str:
    facts: List[str] = []
    for fragment in fragments[:3]:
        source = canonicalize_source_name(fragment.get("source", "")) or "相关媒体"
        raw_fact = strip_report_scaffold(fragment.get("text", "") or fragment.get("title", ""), source)
        if not raw_fact:
            continue
        raw_fact = condense_summary_sentence(raw_fact, limit=96).rstrip("。！？!?")
        if any(titles_match(raw_fact, existing) or raw_fact in existing or existing in raw_fact for existing in facts):
            continue
        facts.append(f"{raw_fact} (信息来源: {source})")

    if not facts:
        headline = condense_summary_sentence(clean_text(candidate.get("headline", "")), limit=96).rstrip("。！？!?")
        primary_source = canonicalize_source_name(str(candidate.get("primary_source", ""))) or "相关媒体"
        if headline:
            facts.append(f"{headline} (信息来源: {primary_source})")

    if timeline:
        facts.append(f"事件含多个明确时间节点，见上方时间线 (信息来源: {facts[0].split('信息来源: ')[-1].rstrip(')') if facts else '相关媒体'})")
    return "；".join(facts[:3]) + "。"


def build_named_view_profile(section_title: str, corpus: str) -> Tuple[str, str, str, str]:
    if corpus_has(corpus, r"(anthropic|openai|ai|model|cyber|人工智能|模型|网络安全|基础设施)"):
        return (
            "美国监管层视角",
            "美国监管层的核心诉求是把模型风险纳入金融与基础设施治理，因为高风险场景一旦出错，责任会落到银行和监管机构身上。",
            "华尔街银行与AI公司视角",
            "银行和AI公司的行为逻辑是争取试点速度，同时证明模型可解释、可审计、可停用；任何安全事件都会拖慢商业化节奏。",
        )
    if corpus_has(corpus, r"(hormuz|shipping|ship|oil|gas|energy|封锁|航运|船只|海峡|能源|油价)"):
        return (
            "美国谈判与能源安全视角",
            "美国的核心诉求是把停火窗口和海上通行风险同时压住，因为运输中断会把军事压力快速转化为能源价格和盟友协调成本。",
            "伊朗与航运市场视角",
            "伊朗的谈判筹码来自地区安全风险上升；运输商、保险方和能源买家会先把不确定性计入运价、保费和库存安排。",
        )
    if corpus_has(corpus, r"(iran|tehran|ceasefire|negotiat|talks?|伊朗|德黑兰|停火|谈判|会谈)"):
        return (
            "美国谈判策略视角",
            "美国的核心诉求是把军事威慑转化为可执行的谈判约束，同时避免冲突成本继续传导到能源、通胀和盟友安全承诺。",
            "伊朗政权安全视角",
            "伊朗的行为逻辑是保留谈判筹码并降低国内安全压力；若让步被解读为被迫退让，其后续执行空间会明显收窄。",
        )
    if corpus_has(corpus, r"(israel|gaza|lebanon|hostage|以色列|加沙|黎巴嫩|人质)"):
        return (
            "以色列安全内阁视角",
            "以色列的核心诉求是维持安全威慑和国内政治支撑，因为停火、边境安排和人质议题都会直接影响政府承压程度。",
            "地区调停方视角",
            "调停方的行为逻辑是先冻结冲突扩散，再争取可验证的执行步骤；任何单方升级都会削弱谈判文本的可信度。",
        )
    if corpus_has(corpus, r"(taiwan|beijing|china|台湾|北京|中国|两岸|反对派)"):
        return (
            "北京政策视角",
            "北京的核心诉求是把两岸议题维持在可塑形的政治框架内，因为对台接触同时服务于内部叙事、外交信号和对美博弈。",
            "台湾与美国政策视角",
            "台湾和美国的行为逻辑是避免象征性接触被转化为既成政治压力；军机军舰、关税或访问安排都会被纳入风险校准。",
        )
    if corpus_has(corpus, r"(tariff|sanction|export control|rare earth|关税|制裁|出口管制|稀土|硫酸|供应)"):
        return (
            "政策制定方视角",
            "政策制定方的核心诉求是用贸易、出口或监管工具重塑供应链约束，因为关键物资一旦短缺，会迅速转化为谈判筹码。",
            "进口商与产业链视角",
            "进口商和下游企业的行为逻辑是提前锁定库存与替代来源；政策落地越突然，价格、合约和生产排期越容易被迫重估。",
        )
    if corpus_has(corpus, r"(fed|private credit|bank|美联储|私人信贷|银行|风险敞口)"):
        return (
            "美联储监管视角",
            "美联储的核心诉求是穿透银行与私人信贷之间的风险传导，因为表外融资扩张会削弱传统资本约束的预警能力。",
            "美国银行风险管理视角",
            "美国银行的行为逻辑是证明风险敞口可计量、可隔离；若监管要求继续加码，资本占用和业务定价都会被迫调整。",
        )
    if corpus_has(corpus, r"(inflation|consumer|sentiment|通胀|消费者|信心|情绪)"):
        return (
            "美国政策制定者视角",
            "政策制定者的核心压力来自通胀预期和消费信心同步恶化，因为这会压缩降息、财政刺激和危机沟通的空间。",
            "消费者与市场定价视角",
            "消费者承受的是价格和就业预期的双重压力；市场会先重估企业利润率、债券收益率和风险资产的折现假设。",
        )
    if section_title == "## 📈【商业与市场 / Business & Markets】":
        return (
            "政策制定者视角",
            "政策制定者的核心诉求是避免单一事件演变成系统性定价压力，因为融资条件、需求预期和风险偏好会互相放大。",
            "投资者定价视角",
            "投资者的行为逻辑是先调整风险溢价，再等待正式数据确认；只要利润或利率路径变动，仓位会比政策声明更早反应。",
        )
    if section_title == "## 🚀【科技与AI / Tech & AI】":
        return (
            "技术部署方视角",
            "技术部署方的核心诉求是扩大采用场景，但必须证明系统在成本、安全和合规上可持续，否则客户试点难以转化为长期合同。",
            "监管与客户视角",
            "监管者和企业客户会把可审计性放在采用速度之前；只要责任边界不清，采购和上线节奏就会被内部风控拖慢。",
        )
    return (
        "直接当事方视角",
        "直接当事方的核心诉求是把眼前压力转化为更有利的谈判或执行条件，因此会优先控制节奏、措辞和可验证动作。",
        "市场与外部观察者视角",
        "市场和外部观察者关注的是事件是否改变成本结构、政策约束或安全预期；信号越模糊，风险溢价越容易先行上升。",
    )


def build_section_view_texts(
    section_title: str,
    fragments: Sequence[Dict[str, str]],
    fallback_labels: Tuple[str, str],
) -> Tuple[str, str, str, str]:
    corpus = clean_text(" ".join(fragment.get("title", "") + " " + fragment.get("text", "") for fragment in fragments))
    if not corpus:
        corpus = " ".join(fallback_labels)
    return build_named_view_profile(section_title, corpus)


def build_ai_inference_text(section_title: str, candidate: Dict[str, object], fragments: Sequence[Dict[str, str]]) -> str:
    corpus = build_candidate_corpus(candidate, fragments)
    if corpus_has(corpus, r"(anthropic|openai|ai|model|cyber|人工智能|模型|网络安全|基础设施)"):
        return (
            "监管层要求银行放慢脚步并非技术保守，而是在把责任边界提前写进部署流程。AI 公司需要证明模型可解释、"
            "可审计、可停用，否则华尔街会把试点保留在沙盒里。"
        )
    if corpus_has(corpus, r"(hormuz|shipping|ship|oil|gas|energy|封锁|航运|船只|海峡|能源|油价)"):
        return (
            "华盛顿需要把停火谈判和航道安全放在同一张表上看。只要船东、保险商和能源买家仍按封锁风险定价，"
            "外交会谈释放的缓和信号就很难真正压低冲突溢价。"
        )
    if corpus_has(corpus, r"(ceasefire|talks?|negotiat|iran|war|停火|谈判|会谈|伊朗|战争)"):
        return (
            "会面本身不足以改变局势，真正有分量的是双方是否同步冻结升级动作。若谈判桌外的军事压力继续增加，"
            "美国和伊朗都会把让步视为弱点，停火文本也会缺少执行可信度。"
        )
    if corpus_has(corpus, r"(taiwan|beijing|china|台湾|北京|中国|两岸)"):
        return (
            "北京的政治接触只有在军事活动同步降温时才会被外界视为缓和。若军机、军舰或关税信号继续升高，"
            "台湾和美国更可能把会面理解为压力管理，而不是关系修复。"
        )
    if corpus_has(corpus, r"(fed|private credit|bank|美联储|私人信贷|银行|风险敞口)"):
        return (
            "美联储追问私人信贷敞口，说明监管已经不满足于看银行表内风险。银行若无法解释风险穿透路径，"
            "下一步会先体现在授信定价和资本占用上，而不是等到违约数据恶化。"
        )
    if corpus_has(corpus, r"(inflation|consumer|sentiment|market|通胀|消费者|信心|市场)"):
        return (
            "通胀和消费者信心同时转弱时，政策沟通会比单一价格数据更难处理。零售、信贷和债券市场会先调整预期，"
            "除非能源与就业数据稳住，否则软着陆叙事会继续失去支撑。"
        )
    if section_title == "## 🚀【科技与AI / Tech & AI】":
        return (
            "技术部署方说速度，客户和监管者说责任，这两者不会自动合流。若试点不能证明收益、成本和安全边界，"
            "AI 公司会被迫收窄应用场景；清晰的审计路径才会重新打开采用曲线。"
        )
    return (
        "这类事件最容易被表态噪音放大，判断时应先看可验证动作而不是措辞强度。当时间表、责任主体和执行路径变清楚，"
        "市场才会把它从政治信号重新定价为实际影响。"
    )


def build_deep_candidate_payload(candidate: Dict[str, object], index: int) -> Dict[str, object]:
    items = []
    for item in candidate.get("items", [])[:5]:
        items.append(
            {
                "source": canonicalize_source_name(str(item.get("source", ""))),
                "title": clean_text(item.get("title", "")),
                "summary": clean_text(item.get("summary", ""))[:360],
                "published": clean_text(str(item.get("published", ""))),
                "link": clean_text(item.get("link", "")),
            }
        )
    return {
        "index": index,
        "headline": clean_text(candidate.get("headline", "")),
        "sources": list(candidate.get("sources", []))[:4],
        "primary_source": canonicalize_source_name(str(candidate.get("primary_source", ""))),
        "source_count": candidate.get("source_count", 0),
        "items": items,
    }


def sanitize_deep_entry_text(value: object, limit: int = 180) -> str:
    text = clean_text(str(value or ""))
    text = re.sub(r"^[-*]\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        text = condense_summary_sentence(text, limit=limit)
    return text


def clean_deep_news_title(value: object) -> str:
    title = clean_label_text(sanitize_deep_entry_text(value, 96))
    title = re.sub(
        r"^(?:Reuters|Bloomberg|Financial Times|FT|WSJ|AP|Associated Press|Al Jazeera|BBC|BBC World News|AFP|Nikkei Asia|South China Morning Post|Techmeme|TechCrunch)[:：]\s*",
        "",
        title,
        flags=re.IGNORECASE,
    )
    title = re.sub(r"^(?:消息人士|知情人士|据悉|据报道|报道称|来源称)[:：]\s*", "", title)
    return clean_label_text(title)


def normalize_core_fact_source_marker(text: object, fallback_source: str) -> str:
    value = clean_text(str(text or ""))
    if "信息来源:" in value:
        return value
    match = re.search(r"\s*[（(]\s*([^)）]{2,48})\s*[)）]\s*$", value)
    if not match:
        return value
    source = clean_label_text(match.group(1))
    canonical_source = canonicalize_source_name(source)
    if looks_like_known_publisher(source) or canonical_source == canonicalize_source_name(fallback_source):
        value = value[:match.start()].rstrip(" 。") + f" (信息来源: {canonical_source})"
    return value


def collect_candidate_citation_refs(candidate: Dict[str, object], limit: int = 6) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    seen = set()
    for item in candidate.get("items", []):
        source = canonicalize_source_name(str(item.get("source", "")))
        title = clean_text(item.get("title", ""))
        url = clean_text(item.get("link", ""))
        if not source or not url or not re.match(r"^https?://", url):
            continue
        key = (source, url)
        if key in seen:
            continue
        refs.append({"source": source, "title": title, "url": url})
        seen.add(key)
        if len(refs) >= limit:
            break
    return refs


def normalize_deep_entry_refs(
    raw_refs: object,
    candidate: Dict[str, object],
    fallback_limit: int = 2,
) -> List[Dict[str, str]]:
    available_refs = collect_candidate_citation_refs(candidate, limit=8)
    url_map = {ref["url"]: ref for ref in available_refs}
    source_map: Dict[str, Dict[str, str]] = {}
    for ref in available_refs:
        source_map.setdefault(ref["source"].lower(), ref)

    normalized: List[Dict[str, str]] = []
    raw_list = raw_refs if isinstance(raw_refs, list) else []
    for raw in raw_list:
        if not isinstance(raw, dict):
            continue
        url = clean_text(raw.get("url", ""))
        source = canonicalize_source_name(str(raw.get("source", "")))
        matched = url_map.get(url) if url else None
        if not matched and source:
            matched = source_map.get(source.lower())
        if not matched:
            continue
        key = (matched["source"], matched["url"])
        if any((item["source"], item["url"]) == key for item in normalized):
            continue
        normalized.append(matched)
        if len(normalized) >= fallback_limit:
            break

    if normalized:
        return normalized
    return available_refs[:fallback_limit]


def render_deep_citation_suffix(refs: Sequence[Dict[str, str]], label: str = "来源") -> str:
    parts = []
    seen = set()
    for ref in refs:
        source = clean_label_text(ref.get("source", ""))
        url = clean_text(ref.get("url", ""))
        if not source or not url or (source, url) in seen:
            continue
        parts.append(f"[{source}]({url})")
        seen.add((source, url))
    if not parts:
        return ""
    return f"（{label}: {', '.join(parts)}）"


def normalize_tangle_deep_entries(
    raw_entries: Sequence[object],
    candidates: Sequence[Dict[str, object]],
    section_title: str,
) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for fallback_index, raw_entry in enumerate(raw_entries, start=1):
        if not isinstance(raw_entry, dict):
            continue
        try:
            entry_index = int(raw_entry.get("index", fallback_index))
        except (TypeError, ValueError):
            entry_index = fallback_index
        candidate_index = entry_index - 1
        if not 0 <= candidate_index < len(candidates):
            candidate_index = fallback_index - 1
        if not 0 <= candidate_index < len(candidates):
            continue
        candidate = candidates[candidate_index]

        title = clean_deep_news_title(raw_entry.get("title"))
        if not title:
            title = clean_deep_news_title(candidate.get("headline", ""))

        overview = sanitize_deep_entry_text(raw_entry.get("overview"), 180)
        if not overview:
            overview = build_section_overview_text(candidate, collect_candidate_report_fragments(candidate), section_title)

        core_fact = sanitize_deep_entry_text(raw_entry.get("core_fact"), 180)
        if not core_fact:
            core_fact = build_core_fact_text(candidate, collect_candidate_report_fragments(candidate), "")

        label_a = clean_label_text(sanitize_deep_entry_text(raw_entry.get("perspective_a_label"), 42)) or "一方观点"
        view_a = sanitize_deep_entry_text(raw_entry.get("perspective_a"), 180)
        label_b = clean_label_text(sanitize_deep_entry_text(raw_entry.get("perspective_b_label"), 42)) or "另一方观点"
        view_b = sanitize_deep_entry_text(raw_entry.get("perspective_b"), 180)
        ai_take = sanitize_deep_entry_text(raw_entry.get("ai_take"), 180)
        overview_refs = normalize_deep_entry_refs(raw_entry.get("overview_refs"), candidate, fallback_limit=2)
        perspective_a_refs = normalize_deep_entry_refs(raw_entry.get("perspective_a_refs"), candidate, fallback_limit=1)
        perspective_b_refs = normalize_deep_entry_refs(raw_entry.get("perspective_b_refs"), candidate, fallback_limit=1)
        ai_take_refs = normalize_deep_entry_refs(raw_entry.get("ai_take_refs"), candidate, fallback_limit=2)

        if not (view_a and view_b and ai_take):
            fragments = collect_candidate_report_fragments(candidate)
            fallback_a, fallback_view_a, fallback_b, fallback_view_b = build_section_view_texts(
                section_title,
                fragments,
                ("一方观点", "另一方观点"),
            )
            label_a = label_a or fallback_a
            view_a = view_a or fallback_view_a
            label_b = label_b or fallback_b
            view_b = view_b or fallback_view_b
            ai_take = ai_take or build_ai_inference_text(section_title, candidate, fragments)

        normalized.append(
            {
                "candidate": candidate,
                "index": fallback_index,
                "title": title,
                "timeline": "",
                "overview": overview,
                "core_fact": core_fact,
                "label_a": label_a,
                "view_a": view_a,
                "label_b": label_b,
                "view_b": view_b,
                "ai_inference": ai_take,
                "overview_refs": overview_refs,
                "perspective_a_refs": perspective_a_refs,
                "perspective_b_refs": perspective_b_refs,
                "ai_take_refs": ai_take_refs,
            }
        )
    return normalized


def build_tangle_style_deep_entries_with_llm(
    section_title: str,
    candidates: Sequence[Dict[str, object]],
) -> List[Dict[str, object]]:
    if os.getenv("DISABLE_TANGLE_DEEP_LLM", "").strip().lower() in {"1", "true", "yes", "on"}:
        return []
    if not candidates:
        return []

    payload = [
        build_deep_candidate_payload(candidate, index)
        for index, candidate in enumerate(candidates[:SECTION_MAX_ITEMS], start=1)
    ]
    prompt = f"""
你是 The Babel Brief 的中文编辑。请模仿 Tangle News 的结构写深读条目：先像 “Today’s topic” 一样概括报道原文，再像 “What one side is saying / What the other side is saying” 一样总结两种观点，最后像 “My take” 一样给出编辑判断。

板块：{section_title}

只返回 JSON 对象：
{{
  "entries": [
    {{
      "index": 1,
      "title": "中文事实标题，不要带媒体名前缀",
      "overview": "80-130字，直接概括报道原文：谁做了什么、发生了什么、报道给出的背景和直接后果",
      "overview_refs": [{{"source": "媒体或信息源名称", "url": "必须原样复制候选 JSON 里的 link"}}],
      "core_fact": "纯事实句；句末括注信息来源媒体名",
      "perspective_a_label": "一方观点标签，例如 美国鹰派怎么看 / 北京怎么看 / 投资者怎么看",
      "perspective_a": "像 Tangle 的观点综述，先概括这一方最强论点，再解释理由；不要复述标题",
      "perspective_a_refs": [{{"source": "媒体、机构、数据源或 X 账号名称", "url": "必须原样复制候选 JSON 里的 link"}}],
      "perspective_b_label": "另一方观点标签，例如 伊朗怎么看 / 台湾怎么看 / 监管者怎么看",
      "perspective_b": "概括另一方、反对方、受影响方或市场反馈的最强论点；没有左右派也要写真实利益冲突",
      "perspective_b_refs": [{{"source": "媒体、机构、数据源或 X 账号名称", "url": "必须原样复制候选 JSON 里的 link"}}],
      "ai_take": "60-100字，像 Tangle 的 My take：先承认双方各自说对了什么，再给出清晰判断",
      "ai_take_refs": [{{"source": "判断所依据的来源名称", "url": "必须原样复制候选 JSON 里的 link"}}]
    }}
  ]
}}

硬规则：
1. 学习 Tangle News 的方法：overview 像 “Today’s topic” 做事实背景；perspective 像 “What one side is saying / What the other side is saying” 收集不同阵营的最强论点；ai_take 像 “My take” 给出编辑判断。
2. 观点材料可以来自候选 JSON 中的媒体报道、机构声明、数据源、Substack、X 等社交媒体链接；但 refs 里的 URL 必须原样复制候选 JSON 的 link，严禁编造、改写或补全 URL。
3. 严禁在 overview 写“这条新闻的重要性在于”“关键变量是”“这条新闻的价值在于”等点评句。overview 只概括报道原文。
4. 严禁把 perspective 写成“Reuters视角 / Bloomberg视角 / Financial Times视角”。观点标签必须是阵营、利益相关方、政策派别、市场群体、机构或具名社交媒体账号。
5. 严禁使用“外部反馈仍需观察”“优势（机会）”“劣势（风险）”“一方面/另一方面”“既有机遇也有挑战”等模板。
6. 不要编造引语；如果原文没有直接引语，就写该方可从报道事实中推导出的最强论点和利益逻辑。
7. title 必须比原始标题更像中文新闻标题，去掉“消息人士”“据报道”“FT:”这类壳。
8. 只使用下面 JSON 里的信息，不要另造事实；不要添加 JSON 里没有出现的武器类型、技术属性、地理范围、机构或专家群体。

候选新闻：
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
    try:
        data = extract_json_object(call_llm(prompt))
    except Exception as exc:
        print(f"[WARN] Tangle 风格深读生成失败，使用本地兜底: {exc}")
        return []

    entries = data.get("entries", [])
    if not isinstance(entries, list):
        return []
    normalized = normalize_tangle_deep_entries(entries, candidates, section_title)
    if len(normalized) < min(len(candidates), SECTION_DEFAULT_ITEMS):
        return []
    return normalized


def collect_traceability_entries(candidate: Dict[str, object], limit: int = 2) -> List[Tuple[str, str, str]]:
    entries: List[Tuple[str, str, str]] = []
    seen = set()
    for item in candidate.get("items", []):
        source = canonicalize_source_name(str(item.get("source", "")))
        title = clean_text(item.get("title", ""))
        url = clean_text(item.get("link", ""))
        key = (source, title, url)
        if not source or not title or not url or key in seen:
            continue
        entries.append((source, title, url))
        seen.add(key)
        if len(entries) >= limit:
            break
    return entries


def build_deep_section_lines_from_candidates(section_title: str, candidates: Sequence[Dict[str, object]]) -> List[str]:
    if not candidates:
        return []

    limited_candidates = list(candidates[:SECTION_MAX_ITEMS])
    entries = build_tangle_style_deep_entries_with_llm(section_title, limited_candidates)

    view_label_map = {
        "## 🇨🇳【中国与世界 / China & The World】": ("直接相关方视角", "外部回应视角"),
        "## 🌍【全球局势 / Global Affairs】": ("直接相关方视角", "外部反馈视角"),
        "## 📈【商业与市场 / Business & Markets】": ("公司/政策方视角", "市场反馈视角"),
        "## 🚀【科技与AI / Tech & AI】": ("公司/技术方视角", "行业反馈视角"),
    }
    label_a, label_b = view_label_map.get(section_title, ("直接相关方视角", "外部反馈视角"))

    if not entries:
        entries = []
        for idx, candidate in enumerate(limited_candidates, start=1):
            headline = clean_deep_news_title(candidate.get("headline", ""))
            fragments = collect_candidate_report_fragments(candidate)
            timeline = build_section_timeline_text(fragments)
            overview = build_section_overview_text(candidate, fragments, section_title)
            core_fact = build_core_fact_text(candidate, fragments, timeline)
            label_a_text, view_a_text, label_b_text, view_b_text = build_section_view_texts(
                section_title,
                fragments,
                (label_a, label_b),
            )
            ai_inference = build_ai_inference_text(section_title, candidate, fragments)
            citation_refs = collect_candidate_citation_refs(candidate, limit=3)

            entries.append(
                {
                    "candidate": candidate,
                    "index": idx,
                    "title": headline,
                    "timeline": timeline,
                    "overview": overview,
                    "core_fact": core_fact,
                    "label_a": label_a_text,
                    "view_a": view_a_text,
                    "label_b": label_b_text,
                    "view_b": view_b_text,
                    "ai_inference": ai_inference,
                    "overview_refs": citation_refs[:2],
                    "perspective_a_refs": citation_refs[:1],
                    "perspective_b_refs": citation_refs[1:2] or citation_refs[:1],
                    "ai_take_refs": citation_refs[:2],
                }
            )
    rendered: List[str] = []

    for entry in entries:
        candidate = entry["candidate"]
        items = candidate.get("items", [])
        primary_source = canonicalize_source_name(str(items[0].get("source", ""))) if items else "来源"
        secondary_source = primary_source
        for item in items[1:]:
            source = canonicalize_source_name(str(item.get("source", "")))
            if source and source != primary_source:
                secondary_source = source
                break

        title = clean_label_text(str(entry["title"]))
        overview = ensure_terminal_punctuation(str(entry["overview"]))
        core_fact = normalize_core_fact_source_marker(entry["core_fact"], primary_source)
        core_fact = ensure_terminal_punctuation(core_fact)
        view_a = ensure_terminal_punctuation(str(entry["view_a"] or entry["overview"]))
        view_b_raw = str(entry["view_b"])
        if not clean_text(view_b_raw):
            view_b_raw = f"围绕该事件的进一步市场与政策反馈仍在形成，后续影响需结合新增交叉报道继续观察。"
        view_b = ensure_terminal_punctuation(view_b_raw)
        ai_inference = ensure_terminal_punctuation(str(entry["ai_inference"]))
        traceability = collect_traceability_entries(candidate, 3)
        overview_suffix = render_deep_citation_suffix(entry.get("overview_refs", []), "来源")
        perspective_a_suffix = render_deep_citation_suffix(entry.get("perspective_a_refs", []), "基于")
        perspective_b_suffix = render_deep_citation_suffix(entry.get("perspective_b_refs", []), "基于")
        ai_suffix = render_deep_citation_suffix(entry.get("ai_take_refs", []), "参考")
        if not perspective_a_suffix:
            perspective_a_suffix = f"（基于: {primary_source} 报道）"
        if not perspective_b_suffix:
            perspective_b_suffix = f"（基于: {secondary_source} 报道）"

        rendered.append(f"### {entry['index']}. {title}")
        rendered.append("")
        rendered.append(f"- 📰 **全景综述**：{overview}{overview_suffix}")
        if clean_text(str(entry.get("timeline", ""))):
            rendered.append(f"- ⏱️ **新闻时间线**：{entry['timeline']}")
        core_fact_suffix = "" if "信息来源:" in core_fact else f" (信息来源: {primary_source})"
        rendered.append(f"- 📌 **核心事实**：{core_fact}{core_fact_suffix}")
        rendered.append(f"- 🔴 **{entry['label_a']}**：{view_a}{perspective_a_suffix}")
        rendered.append(f"- 🔵 **{entry['label_b']}**：{view_b}{perspective_b_suffix}")
        rendered.append(f"- ⚖️ **AI推演**：{ai_inference}{ai_suffix}")
        rendered.append("- 🔗 **溯源印证**：")
        for source, trace_title, url in traceability:
            rendered.append(f"    * **{source}**: [{trace_title}]({url})")
        rendered.append("")

    return rendered


def ensure_structured_deep_sections(md_text: str, selections: Dict[str, object]) -> str:
    section_key_map = {
        "## 🇨🇳【中国与世界 / China & The World】": "china_focus_candidates",
        "## 🌍【全球局势 / Global Affairs】": "global_affairs_candidates",
        "## 📈【商业与市场 / Business & Markets】": "business_market_candidates",
        "## 🚀【科技与AI / Tech & AI】": "tech_ai_candidates",
    }

    rebuilt_text = md_text
    for section_title, candidate_key in section_key_map.items():
        candidates = selections.get(candidate_key, [])
        replacement = build_deep_section_lines_from_candidates(section_title, candidates)
        if not replacement:
            rebuilt_text = remove_markdown_section(rebuilt_text, section_title)
            continue
        rebuilt_text = replace_markdown_section(rebuilt_text, section_title, replacement)
    return rebuilt_text


def normalize_history_line(text: str) -> str:
    value = text.strip()
    if value.startswith('<p class="history-note">'):
        return value
    value = re.sub(r"^#+\s*", "", value)
    value = value.lstrip(">").strip()
    value = re.sub(r"^\*\*(历史上的今天（[^）]+）)\*\*", r"\1", value)

    label = "历史上的今天"
    body = value
    if "：" in value:
        head, tail = value.split("：", 1)
        if head.startswith("历史上的今天"):
            label = head.strip()
            body = tail.strip()
    elif ":" in value:
        head, tail = value.split(":", 1)
        if head.startswith("历史上的今天"):
            label = head.strip()
            body = tail.strip()

    link_match = re.search(r"\[([^\]]+)\]\((https?://[^)]+)\)\s*$", body)
    link_html = ""
    if link_match:
        link_label = html.escape(link_match.group(1).strip())
        link_url = html.escape(link_match.group(2).strip(), quote=True)
        body = body[:link_match.start()].rstrip(" -—–")
        link_html = f' —— <a href="{link_url}">{link_label}</a>'

    safe_label = html.escape(label)
    safe_body = html.escape(body.strip())
    return f'<p class="history-note"><span class="history-label">{safe_label}</span>：{safe_body}{link_html}</p>'


def normalize_report_markdown(md_text: str, source_catalog: Sequence[Dict[str, object]] = ()) -> str:
    lines = md_text.splitlines()
    normalized: List[str] = []
    in_deep_section = False
    index = 0

    while index < len(lines):
        raw = lines[index]
        stripped = raw.strip()

        if "历史上的今天" in stripped:
            history_text = stripped
            next_index = index + 1
            if re.match(r"^>?[\s#*]*历史上的今天[:：]?\s*$", stripped):
                while next_index < len(lines) and not lines[next_index].strip():
                    next_index += 1
                if next_index < len(lines):
                    candidate = lines[next_index].strip().lstrip(">").strip()
                    if candidate and not candidate.startswith("## ") and candidate != "---":
                        history_text = f"历史上的今天：{candidate}"
                        index = next_index
            normalized.append(normalize_history_line(history_text))
            normalized.append("")
            index += 1
            continue

        if stripped == QUICK_HITS_TITLE:
            normalized.append(QUICK_HITS_TITLE)
            normalized.append("")
            index += 1
            quick_hits_buffer: List[str] = []
            while index < len(lines):
                candidate = lines[index].strip()
                if candidate.startswith("## ") and candidate != QUICK_HITS_TITLE:
                    break
                quick_hits_buffer.append(lines[index])
                index += 1
            normalized.extend(normalize_quick_hits_entries(quick_hits_buffer))
            normalized.append("")
            in_deep_section = False
            continue

        if stripped in DEEP_SECTION_TITLES:
            in_deep_section = True
            normalized.append(stripped)
            normalized.append("")
            index += 1
            continue

        if stripped.startswith("## "):
            in_deep_section = stripped in DEEP_SECTION_TITLES
            normalized.append(stripped)
            index += 1
            continue

        if not in_deep_section:
            normalized.append(raw)
            index += 1
            continue

        if not stripped:
            if normalized and normalized[-1] != "":
                normalized.append("")
            index += 1
            continue

        ordered_match = re.match(r"^(\d+)\.\s+(.*)$", stripped)
        content = ordered_match.group(2).strip() if ordered_match else stripped

        parsed_field = normalize_deep_field_line(content)
        if parsed_field:
            if "客观共识与风险推演" in parsed_field or "AI推演" in parsed_field:
                ai_body = parsed_field.split("：", 1)[1].strip() if "：" in parsed_field else ""
                if ai_body:
                    normalized.append(parsed_field)
                    index += 1
                else:
                    merged_line, next_index = consume_ai_lines(lines, index)
                    normalized.append(merged_line)
                    index = next_index
                continue
            if "溯源印证" in parsed_field:
                _, body = split_label_and_body(content)
                source_block, next_index = consume_source_lines(lines, index, body, source_catalog)
                normalized.extend(source_block)
                normalized.append("")
                index = next_index
                continue
            normalized.append(parsed_field)
            index += 1
            continue

        if ordered_match:
            title = clean_label_text(re.sub(r"^###\s*\d+\.\s*", "", content))
            normalized.append(f"### {ordered_match.group(1)}. {title}")
            normalized.append("")
            index += 1
            continue

        if re.match(r"^###\s+\d+\.\s+", stripped):
            normalized.append(f"### {clean_label_text(stripped)}")
            normalized.append("")
            index += 1
            continue

        if re.match(r"^\*\*.+\*\*$", stripped):
            normalized.append(f"### {strip_md_wrappers(stripped)}")
            normalized.append("")
            index += 1
            continue

        pseudo_heading_match = re.match(
            r"^(?:[-*]\s+)?(?:\*\*|__)?(\d+)\.\s+(.+?)(?:\*\*|__)?(?:[:：].*)?$",
            stripped,
        )
        if pseudo_heading_match:
            title = clean_label_text(pseudo_heading_match.group(2))
            if title and not re.search(r"(全景综述|核心事实|视角|客观共识|风险推演|AI推演|溯源印证)", title):
                normalized.append(f"### {pseudo_heading_match.group(1)}. {title}")
                normalized.append("")
                index += 1
                continue

        normalized.append(raw)
        index += 1

    return "\n".join(normalized).strip()


def build_cover_image_url(image_prompt: str) -> str:
    return ""


def infer_smtp_host(raw_host: str, username: str) -> str:
    host = clean_text(raw_host)
    username = username.strip().lower()

    if host and "@" not in host:
        return host
    if username.endswith("@gmail.com"):
        return "smtp.gmail.com"
    if username.endswith(("@outlook.com", "@hotmail.com", "@live.com")):
        return "smtp-mail.outlook.com"
    if username.endswith("@qq.com"):
        return "smtp.qq.com"
    return host


def normalize_smtp_password(password: str, username: str) -> str:
    password = (password or "").replace("\xa0", " ").strip()
    if username.strip().lower().endswith("@gmail.com"):
        return re.sub(r"\s+", "", password)
    return password


def build_sender_addresses(raw_sender: str, username: str) -> Tuple[str, str]:
    sender = clean_text(raw_sender)
    username = username.strip()

    if sender and "@" in sender:
        return sender, sender
    if sender:
        return formataddr((sender, username)), username
    return username, username


def send_email(subject: str, html_content: str) -> None:
    host = infer_smtp_host(os.getenv("SMTP_HOST", ""), os.getenv("SMTP_USERNAME", ""))
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USERNAME", "").strip()
    pwd = normalize_smtp_password(os.getenv("SMTP_PASSWORD", ""), user)
    header_from, envelope_from = build_sender_addresses(os.getenv("EMAIL_FROM", ""), user)
    to_addrs = [value.strip() for value in os.getenv("EMAIL_TO", "").split(",") if value.strip()]

    if not all([host, user, pwd]) or not to_addrs:
        raise RuntimeError("SMTP/EMAIL 配置不完整，无法发送邮件。")

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = header_from
    message["To"] = ", ".join(to_addrs)
    message.attach(MIMEText(html_content, "html", "utf-8"))

    context = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=context) as server:
            server.login(user, pwd)
            server.sendmail(envelope_from, to_addrs, message.as_string())
        return

    with smtplib.SMTP(host, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(user, pwd)
        server.sendmail(envelope_from, to_addrs, message.as_string())


def get_retry_delay_seconds(attempt: int) -> int:
    base_delay = max(5, int(os.getenv("RETRY_BASE_DELAY_SECONDS", "20")))
    max_delay = max(base_delay, int(os.getenv("RETRY_MAX_DELAY_SECONDS", "300")))
    return min(max_delay, base_delay * max(1, attempt))


def run_brief_once() -> None:
    print(f"[{get_local_now().strftime('%H:%M:%S')}] 🚀 启动 The Babel Brief 终极版引擎...")
    os.makedirs("archives", exist_ok=True)
    assert_outbound_network_ready()

    # 1. 并发抓取
    all_news = []
    timeout_s = int(os.getenv("HTTP_TIMEOUT_SECONDS", "25"))
    lookback_hours = int(os.getenv("LOOKBACK_HOURS", "24"))
    fetch_workers = int(os.getenv("FETCH_WORKERS", "8"))
    with ThreadPoolExecutor(max_workers=fetch_workers) as executor:
        futures = [executor.submit(fetch_rss, name, url, timeout_s) for name, url in DEFAULT_SOURCES.items()]
        for f in as_completed(futures):
            all_news.extend(f.result())

    # 2. 聚类分析与历史调取
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    fresh_news = [item for item in all_news if item.published is None or item.published >= cutoff]
    if not fresh_news:
        raise RuntimeError("未抓取到任何有效新闻，简报生成已中止。")

    clusters = sorted(cluster_items(fresh_news), key=cluster_priority_key, reverse=True)
    source_catalog = [
        serialize_cluster(
            cluster,
            max_items=max(3, get_prompt_items_per_cluster()),
            summary_limit=max(180, get_prompt_summary_limit()),
        )
        for cluster in clusters[: get_source_catalog_limit()]
    ]
    print("[INFO] 正在使用 Gemini 重要性评分器为候选新闻打分...")
    attach_llm_importance_scores(source_catalog)
    selections = build_editorial_selections(source_catalog)
    validate_editorial_selections(selections)
    history = fetch_wikipedia_on_this_day(timeout_s)
    fallback_subject = build_default_subject(clusters)

    # 3. 调用 AI 主编
    prompt = build_prompt(source_catalog, history)
    md_report = call_llm(prompt)

    # 4. 解析元数据与渲染 HTML
    subject, image_prompt, clean_md = extract_report_metadata(md_report, fallback_subject)
    clean_md = normalize_report_markdown(clean_md, source_catalog)
    clean_md = ensure_quick_hits_section(clean_md, selections)
    if report_needs_structural_repair(clean_md, selections):
        if should_avoid_heavy_llm_repairs():
            print("[INFO] 检测到版式异常，跳过重型 LLM 终校，改用本地结构重建以降低失败概率。")
        else:
            print("[INFO] 检测到版式异常，启动二次终校修复...")
            repaired_md = repair_report_structure(clean_md, history, selections)
            clean_md = normalize_report_markdown(repaired_md, source_catalog)
            clean_md = ensure_quick_hits_section(clean_md, selections)
    clean_md = ensure_structured_deep_sections(clean_md, selections)
    clean_md = normalize_report_markdown(clean_md, source_catalog)
    clean_md = ensure_quick_hits_section(clean_md, selections)
    if report_has_english_heading_or_field_residue(clean_md):
        if should_avoid_heavy_llm_repairs():
            print("[INFO] 检测到深读板块英文残留，执行必要中文化；继续跳过重型结构重写。")
        else:
            print("[INFO] 正在执行终稿中文化校验...")
        clean_md = translate_remaining_english_headings(clean_md)
        clean_md = translate_remaining_english_fields(clean_md)
    else:
        print("[INFO] 深读标题与字段已是中文，跳过末端英文终校。")
    clean_md = normalize_report_markdown(clean_md, source_catalog)
    clean_md = ensure_quick_hits_section(clean_md, selections)
    if quick_hits_has_english_residue(clean_md):
        print("[INFO] Quick Hits 检测到英文残留，执行定向中文化。")
    clean_md = translate_remaining_quick_hits(clean_md)
    clean_md = polish_markdown_field_artifacts(clean_md)
    clean_md = normalize_rendered_source_suffixes(clean_md)
    clean_md = ensure_deep_source_suffixes(clean_md, selections)
    clean_md = ensure_verified_today_echo(clean_md, timeout_s)
    validate_rendered_report(clean_md, selections)
    cover_url = build_cover_image_url(image_prompt)
    html_report = render_email_html(clean_md, cover_url, subject)

    # 5. 物理归档逻辑
    today_fn = get_local_now().strftime("%Y-%m-%d")
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_report)
    with open(f"archives/{today_fn}.html", "w", encoding="utf-8") as f:
        f.write(html_report)

    # 6. 发送邮件
    print(f"[INFO] 正在向 {os.getenv('EMAIL_TO')} 发送简报...")
    send_email(subject, html_report)
    print(f"✨ 任务完美达成！简报已发送至邮箱并存档至 archives/{today_fn}.html")


# =============================================================================
# § 7  主程序 (并发抓取与像素归档)
# =============================================================================

def main():
    load_dotenv()
    maybe_enable_socks_proxy()
    attempt = 1
    while True:
        try:
            print(f"[INFO] 本轮发送尝试 #{attempt}")
            run_brief_once()
            return
        except Exception as exc:
            delay = get_retry_delay_seconds(attempt)
            print(f"[ERROR] 第 {attempt} 次发送失败: {exc}")
            traceback.print_exc()
            print(f"[INFO] 将在 {delay} 秒后自动重试，直到发送成功。")
            time.sleep(delay)
            attempt += 1

if __name__ == "__main__":
    main()
