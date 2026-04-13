from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from html import escape, unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


BASE_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = BASE_DIR / "archives"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
ARCHIVE_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
NON_HEADLINE_LABELS = {
    "AI推演",
    "Bloomberg视角",
    "Editor",
    "Editor's Notes",
    "Reuters视角",
    "WSJ视角",
    "今日回响",
    "全景综述",
    "市场反馈视角",
    "核心事实",
    "溯源印证",
    "行业反馈视角",
}

MONTH_NAMES = {
    1: "Jan.",
    2: "Feb.",
    3: "Mar.",
    4: "Apr.",
    5: "May",
    6: "Jun.",
    7: "Jul.",
    8: "Aug.",
    9: "Sep.",
    10: "Oct.",
    11: "Nov.",
    12: "Dec.",
}

BACK_LINK_HTML = """
<div class="brief-backbar">
    <a class="brief-brand" href="/">The Babel Brief</a>
    <a class="brief-back-link" href="/">&lt; back</a>
    <button class="theme-toggle" type="button" data-theme-toggle>[ DARK ]</button>
</div>
"""

DETAIL_CLOCK_SCRIPT = """
<script>
(() => {
    const clocks = document.querySelectorAll("[data-cn-clock]");
    if (clocks.length) {
        const timeFormatter = new Intl.DateTimeFormat("zh-CN", {
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "Asia/Shanghai"
        });
        const dateFormatter = new Intl.DateTimeFormat("zh-CN", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            timeZone: "Asia/Shanghai"
        });
        const tick = () => {
            const now = new Date();
            clocks.forEach((clock) => {
                clock.textContent = timeFormatter.format(now);
                clock.classList.remove("is-ticking");
                void clock.offsetWidth;
                clock.classList.add("is-ticking");
                setTimeout(() => clock.classList.remove("is-ticking"), 120);
            });
            document.querySelectorAll("[data-cn-date]").forEach((date) => {
                date.textContent = dateFormatter.format(now).replace(/\\//g, " / ");
            });
        };

        tick();
        setInterval(tick, 30000);
    }

    const themeKey = "babel-brief-theme";
    const buttons = document.querySelectorAll("[data-theme-toggle]");
    const storedTheme = localStorage.getItem(themeKey);
    const hashTheme = new URLSearchParams(window.location.hash.slice(1)).get("figmatheme");
    const forcedTheme = ["dark", "light"].includes(hashTheme) ? hashTheme : "";
    const initialTheme = forcedTheme || storedTheme || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    const setTheme = (theme) => {
        document.documentElement.dataset.theme = theme;
        localStorage.setItem(themeKey, theme);
        buttons.forEach((button) => {
            button.textContent = theme === "dark" ? "[ LIGHT ]" : "[ DARK ]";
            button.setAttribute("aria-label", theme === "dark" ? "Switch to light mode" : "Switch to dark mode");
        });
    };

    setTheme(initialTheme);
    buttons.forEach((button) => {
        button.addEventListener("click", () => {
            setTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark");
        });
    });
})();
</script>
"""

DETAIL_STYLE_HTML = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Doto:wght@400;700&family=Space+Grotesk:wght@300;400;500;700&family=Space+Mono:wght@400;700&display=swap');
:root {
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
    --accent: #FF661F;
    --interactive: var(--text-display);
    --display-xl: 4.5rem;
    --display-lg: 3rem;
    --display-md: 2.25rem;
    --heading: 1.5rem;
    --subheading: 1.125rem;
    --body: 1rem;
    --body-sm: 0.875rem;
    --caption: 0.75rem;
    --label: 0.6875rem;
    --button: 0.8125rem;
    --space-2xs: 2px;
    --space-xs: 4px;
    --space-sm: 8px;
    --space-md: 16px;
    --space-lg: 24px;
    --space-xl: 32px;
    --space-2xl: 48px;
    --space-3xl: 64px;
    --space-4xl: 96px;
    --motion-fast: 160ms;
    --motion-base: 240ms;
    --motion-slow: 360ms;
    --ease-out: cubic-bezier(0.25, 0.1, 0.25, 1);
}
:root[data-theme="dark"] {
    color-scheme: dark;
    --black: #000000;
    --surface: #111111;
    --surface-raised: #1A1A1A;
    --border: #222222;
    --border-visible: #333333;
    --text-disabled: #666666;
    --text-secondary: #999999;
    --text-primary: #E8E8E8;
    --text-display: #FFFFFF;
    --interactive: var(--text-display);
}
* {
    box-sizing: border-box;
    letter-spacing: 0;
}
body {
    margin: 0;
    padding: var(--space-4xl) var(--space-xl);
    background: var(--black);
    color: var(--text-primary);
    font-family: "Space Grotesk", "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: var(--body);
    line-height: 1.5;
    transition: background-color var(--motion-base) var(--ease-out), color var(--motion-base) var(--ease-out);
}
.brief-backbar {
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    column-gap: var(--space-lg);
    max-width: 1200px;
    margin: 0 auto calc(var(--space-xl) + var(--space-sm));
    padding-bottom: var(--space-md);
    border-bottom: 1px solid var(--border-visible);
}
.brief-backbar .brief-back-link {
    grid-column: 4 / 9;
    width: fit-content;
    color: var(--accent);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 700;
    text-decoration: none;
    text-transform: uppercase;
    border-bottom: 0;
    transition: color var(--motion-fast) var(--ease-out), border-color var(--motion-fast) var(--ease-out);
}
.theme-toggle {
    grid-column: 10 / 13;
    justify-self: end;
    align-self: start;
    min-height: 44px;
    padding: 12px 24px;
    background: transparent;
    border: 1px solid var(--border-visible);
    border-radius: 999px;
    color: var(--text-display);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--button);
    line-height: 1.4;
    font-weight: 700;
    text-transform: uppercase;
    cursor: pointer;
    transition: border-color var(--motion-fast) var(--ease-out), color var(--motion-fast) var(--ease-out), background-color var(--motion-fast) var(--ease-out);
}
.theme-toggle:hover,
.theme-toggle:focus-visible {
    border-color: var(--text-display);
}
.brief-backbar .brief-brand {
    grid-column: 1 / 3;
    width: fit-content;
    padding-left: 14px;
    color: var(--accent);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 700;
    text-decoration: none;
    text-transform: uppercase;
    border-bottom: 0;
}
.brief-backbar .brief-brand:hover,
.brief-backbar .brief-brand:focus-visible {
    text-decoration: underline;
    text-underline-offset: 4px;
}
.email-container {
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    column-gap: var(--space-lg);
    row-gap: 0;
    align-items: start;
    max-width: 1200px;
    margin: 0 auto;
    padding: 0;
    background: transparent;
    border: 0;
    border-radius: 0;
    box-shadow: none;
}
.brief-detail-header {
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    column-gap: var(--space-lg);
    max-width: 1200px;
    margin: 0 auto var(--space-3xl);
}
.brief-detail-instruments {
    grid-column: 1 / 3;
    display: grid;
    gap: var(--space-sm);
    align-content: start;
}
.instrument-label {
    margin: 0;
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 700;
    text-transform: uppercase;
}
.instrument-number {
    margin: 0;
    color: var(--text-display);
    font-family: "Doto", "Space Mono", monospace;
    font-size: var(--display-lg);
    line-height: 0.95;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
}
.instrument-meta,
.instrument-clock {
    margin: 0;
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 400;
    text-transform: uppercase;
    font-variant-numeric: tabular-nums;
}
.instrument-clock {
    width: fit-content;
    margin-top: var(--space-md);
    padding-top: var(--space-sm);
    border-top: 1px solid var(--border-visible);
    color: var(--text-display);
    transition: opacity var(--motion-fast) var(--ease-out), color var(--motion-fast) var(--ease-out);
}
.instrument-clock.is-ticking {
    opacity: 0.55;
}
.brief-detail-title {
    grid-column: 4 / 12;
    align-self: center;
    margin: 0;
    width: min(788.268px, 100%);
    max-width: 100%;
    opacity: 1;
    color: var(--text-display);
    font-size: 32px;
    line-height: 1.15;
    font-weight: 700;
    display: -webkit-box;
    overflow: hidden;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 2;
}
.email-container > * {
    grid-column: 4 / 12;
    max-width: 65ch;
}
.hero,
.email-container > hr {
    grid-column: 1 / -1;
    max-width: none;
}
.email-container > h2 {
    grid-column: 4 / 12;
    max-width: 65ch;
}
.hero {
    margin: 0 0 var(--space-3xl);
}
.hero img {
    width: 100%;
    display: block;
    border-radius: 0;
}
h1 {
    max-width: 12em;
    margin: 0 0 var(--space-xl);
    padding: 0;
    border: 0;
    color: var(--text-display);
    font-size: var(--heading);
    line-height: 1.2;
    font-weight: 700;
    letter-spacing: 0;
}
h2 {
    margin: var(--space-3xl) 0 var(--space-lg);
    padding: var(--space-md) 0 0;
    border-top: 1px solid var(--border-visible);
    border-left: 0;
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: 24px;
    line-height: 1.2;
    font-weight: 700;
    text-transform: uppercase;
}
h2:first-of-type {
    margin-top: var(--space-2xl);
}
h3 {
    margin: var(--space-xl) 0 var(--space-md);
    color: var(--accent);
    font-family: "Space Grotesk", "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: var(--body);
    line-height: 1.3;
    font-weight: 700;
}
p,
li {
    color: var(--text-primary);
    font-size: var(--body);
    line-height: 1.6;
}
p {
    margin: 0 0 var(--space-md);
}
ul {
    margin: 0 0 var(--space-lg);
    padding: 0;
    list-style: none;
    border-top: 1px solid var(--border);
}
li {
    margin: 0;
    padding: 12px 0;
    border-bottom: 1px solid var(--border);
}
ul ul {
    margin-top: var(--space-sm);
    margin-bottom: 0;
    padding-left: var(--space-lg);
    border-top: 1px solid var(--border);
}
.history-note {
    margin: 0 0 var(--space-xl);
    color: var(--text-primary);
    font-size: var(--body);
    line-height: 1.5;
}
.history-label {
    color: var(--text-display);
    font-weight: 700;
}
blockquote {
    margin: var(--space-lg) 0;
    padding: var(--space-md);
    background: transparent;
    border: 1px solid var(--border-visible);
    border-radius: 4px;
    color: var(--text-primary);
}
a {
    color: var(--interactive);
    text-decoration: none;
    border-bottom: 1px solid currentColor;
    transition: color var(--motion-fast) var(--ease-out), border-color var(--motion-fast) var(--ease-out);
}
a:hover {
    color: var(--text-display);
    border-bottom-color: var(--text-display);
}
strong {
    color: var(--text-display);
    font-weight: 700;
}
.email-container > ul:first-of-type > li > strong {
    color: var(--accent);
}
.email-container > ul:first-of-type {
    border-top: 0;
}
.email-container > ul:first-of-type > li {
    border-bottom: 0;
}
code {
    padding: 1px 4px;
    background: var(--surface-raised);
    border: 1px solid var(--border-visible);
    border-radius: 4px;
    color: var(--text-display);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
}
hr {
    border: 0;
    border-top: 1px solid var(--border-visible);
    margin: var(--space-3xl) 0;
}
@media (prefers-reduced-motion: no-preference) {
    @keyframes nd-power-on {
        from { opacity: 0; }
        to { opacity: 1; }
    }
    .brief-backbar,
    .brief-detail-header,
    .email-container {
        animation: nd-power-on var(--motion-base) var(--ease-out) both;
    }
    .brief-detail-header {
        animation-delay: 60ms;
    }
    .email-container {
        animation-delay: 120ms;
    }
}
@media (prefers-reduced-motion: reduce) {
    *,
    *::before,
    *::after {
        animation-duration: 1ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 1ms !important;
        scroll-behavior: auto !important;
    }
}
@media (max-width: 640px) {
    body {
        padding: var(--space-3xl) var(--space-md);
    }
    .brief-backbar {
        grid-template-columns: 1fr;
        gap: var(--space-md);
        margin-bottom: var(--space-2xl);
    }
    .brief-backbar span,
    .brief-backbar a,
    .theme-toggle {
        grid-column: 1;
    }
    .theme-toggle {
        justify-self: start;
    }
    .email-container {
        display: block;
    }
    .brief-detail-header {
        grid-template-columns: 1fr;
        gap: var(--space-md);
        margin-bottom: var(--space-3xl);
    }
    .brief-detail-instruments,
    .brief-detail-title {
        grid-column: 1;
    }
    .email-container > * {
        max-width: none;
    }
    h1 {
        font-size: var(--heading);
    }
    .brief-detail-title {
        font-size: var(--heading);
    }
    h2 {
        margin-top: var(--space-2xl);
    }
    h3 {
        font-size: var(--body);
    }
}
</style>
"""

DECORATIVE_MARKERS = (
    "🤖 ",
    "📰 ",
    "📌 ",
    "🔴 ",
    "🔵 ",
    "⚖️ ",
    "🔗 ",
    "🚨 ",
    "🚀",
    "🌍",
    "📈",
    "🇨🇳",
    "🎵 ",
    "🎤 ",
    "📅 ",
    "💽 ",
    "📝 ",
    "⏱️ ",
    "➡️ ",
)

DETAIL_HEADING_LABELS = {
    "Quick Hits": "🦉 Quick Hits",
    "中国与世界 / China & The World": "🇨🇳 中国与世界 / China & The World",
    "全球局势 / Global Affairs": "🌍 全球局势 / Global Affairs",
    "商业与市场 / Business & Markets": "💰 商业与市场 / Business & Markets",
    "科技与AI / Tech & AI": "🚀 科技与AI / Tech & AI",
    "今日回响": "今日回响",
}


app = FastAPI(title="The Babel Brief")


@dataclass(frozen=True)
class ArchiveEntry:
    archive_date: Date
    title: str


class TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_title = False
        self._in_heading = False
        self._in_h2 = False
        self._in_strong = False
        self._after_quick_hits_heading = False
        self._in_quick_hits_list = False
        self._in_quick_hits_item = False
        self._quick_hits_depth = 0
        self._parts: list[str] = []
        self._heading_parts: list[str] = []
        self._h2_parts: list[str] = []
        self._strong_parts: list[str] = []
        self._quick_hits_parts: list[str] = []
        self._heading_headlines: list[str] = []
        self._strong_headlines: list[str] = []
        self._quick_hit_summary = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = True
        if tag_name == "h2":
            self._in_h2 = True
            self._h2_parts = []
        if tag_name == "ul" and self._after_quick_hits_heading and not self._quick_hit_summary:
            self._in_quick_hits_list = True
            self._after_quick_hits_heading = False
            self._quick_hits_depth = 1
        elif tag_name == "ul" and self._in_quick_hits_list:
            self._quick_hits_depth += 1
        if tag_name == "li" and self._in_quick_hits_list and self._quick_hits_depth == 1 and not self._quick_hit_summary:
            self._in_quick_hits_item = True
            self._quick_hits_parts = []
        if tag_name in {"h1", "h3"}:
            self._in_heading = True
            self._heading_parts = []
        if tag_name == "strong":
            self._in_strong = True
            self._strong_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = False
        if tag_name == "h2":
            self._in_h2 = False
            heading = normalize_text("".join(self._h2_parts))
            self._after_quick_hits_heading = "Quick Hits" in heading or "【Quick Hits】" in heading
        if tag_name == "li" and self._in_quick_hits_item:
            self._in_quick_hits_item = False
            self._quick_hit_summary = clean_archive_summary("".join(self._quick_hits_parts))
        if tag_name == "ul" and self._in_quick_hits_list:
            self._quick_hits_depth -= 1
            if self._quick_hits_depth <= 0:
                self._in_quick_hits_list = False
        if tag_name in {"h1", "h3"}:
            self._in_heading = False
            candidate = normalize_headline("".join(self._heading_parts))
            if candidate and len(self._heading_headlines) < 2:
                self._heading_headlines.append(candidate)
        if tag_name == "strong":
            self._in_strong = False
            candidate = normalize_text("".join(self._strong_parts))
            if candidate and candidate not in NON_HEADLINE_LABELS and len(self._strong_headlines) < 2:
                self._strong_headlines.append(candidate)

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._parts.append(data)
        if self._in_h2:
            self._h2_parts.append(data)
        if self._in_heading:
            self._heading_parts.append(data)
        if self._in_strong:
            self._strong_parts.append(data)
        if self._in_quick_hits_item:
            self._quick_hits_parts.append(data)

    @property
    def title(self) -> str:
        return normalize_text("".join(self._parts))

    @property
    def quick_hit_summary(self) -> str:
        return self._quick_hit_summary

    @property
    def fallback_title(self) -> str:
        headlines = self._heading_headlines or self._strong_headlines
        return headlines[0] if headlines else ""


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_headline(text: str) -> str:
    return re.sub(r"^\d+\.\s*", "", normalize_text(text))


def remove_source_attribution_words(text: str) -> str:
    value = normalize_text(text)
    value = re.sub(r"^\s*(?:消息人士|知情人士|消息人士称|知情人士称|据消息人士(?:称|透露)?|据知情人士(?:称|透露)?)\s*[:：,，]\s*", "", value)
    value = re.sub(r"^\s*(?:消息人士|知情人士)(?:称|透露)\s*[,，]\s*", "", value)
    return normalize_text(value)


def shorten_archive_title(text: str, limit: int = 48) -> str:
    value = remove_source_attribution_words(text)
    if "：" in value or ":" in value:
        headline, summary = re.split(r"[:：]", value, maxsplit=1)
        headline = remove_source_attribution_words(headline)
        summary = remove_source_attribution_words(summary)
        value = headline if len(headline) >= 10 else summary or headline

    value = remove_source_attribution_words(value).rstrip("。！？!?")
    if len(value) <= limit:
        return value

    shortened = value[:limit].rstrip(" ，,；;：:")
    if (
        re.search(r"[A-Za-z0-9]$", shortened)
        and limit < len(value)
        and re.match(r"[A-Za-z0-9]", value[limit:limit + 1] or "")
    ):
        word_safe = re.sub(r"[A-Za-z0-9_-]+$", "", shortened).rstrip(" ，,；;：:")
        if len(word_safe) >= max(16, limit // 2):
            shortened = word_safe
    return shortened


def clean_archive_summary(text: str) -> str:
    value = normalize_text(text)
    for marker in DECORATIVE_MARKERS:
        value = value.replace(marker, "")
    value = re.sub(r"^\s*`?\[独家重磅\]`?\s*", "", value)
    value = re.sub(r"（\s*来源[:：].*?）\s*$", "", value)
    value = re.sub(r"\s*\[+\s*来源[:：].*$", "", value)
    value = re.sub(r"\s*来源[:：].*$", "", value)
    value = value.strip(" ：:，,；;[]")
    return shorten_archive_title(value) or value


def extract_title(html_content: str) -> str:
    parser = TitleParser()
    parser.feed(html_content)
    parser.close()
    fallback = "" if parser.title == "The Babel Brief" else parser.title
    return parser.quick_hit_summary or parser.fallback_title or fallback or "The Babel Brief"


def read_archive_entries() -> list[ArchiveEntry]:
    if not ARCHIVE_DIR.exists():
        return []

    entries: list[ArchiveEntry] = []
    for archive_path in ARCHIVE_DIR.glob("*.html"):
        if not ARCHIVE_NAME_RE.fullmatch(archive_path.stem):
            continue

        try:
            archive_date = Date.fromisoformat(archive_path.stem)
        except ValueError:
            continue

        html_content = archive_path.read_text(encoding="utf-8", errors="replace")
        entries.append(ArchiveEntry(archive_date=archive_date, title=extract_title(html_content)))

    return sorted(entries, key=lambda entry: entry.archive_date, reverse=True)


def render_index(entries: Sequence[ArchiveEntry]) -> str:
    now = datetime.now(LOCAL_TZ)
    today = now.date()
    clock_text = now.strftime("%H:%M")
    clock_date = now.strftime("%Y / %m / %d")
    latest_entry = entries[0] if entries else None
    latest_day = f"{latest_entry.archive_date.day:02d}" if latest_entry else "--"
    latest_date = latest_entry.archive_date.strftime("%Y / %m / %d") if latest_entry else "-- / -- / --"
    archive_count = f"{len(entries):02d}"
    rows = "\n".join(render_archive_row(entry, today, index) for index, entry in enumerate(entries, start=1))
    if not rows:
        rows = '<p class="empty">暂无归档</p>'

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>The Babel Brief</title>
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
    --accent: #FF661F;
    --home-title-size: 48px;
    --home-title-line-height: 50px;
    --home-title-weight: 500;
    --home-label-color: rgba(102, 102, 102, 0.5);
    --home-label-size: 12px;
    --interactive: var(--text-display);
    --display-xl: 4.5rem;
    --display-lg: 3rem;
    --display-md: 2.25rem;
    --heading: 1.5rem;
    --subheading: 1.125rem;
    --body: 1rem;
    --body-sm: 0.875rem;
    --caption: 0.75rem;
    --label: 0.6875rem;
    --button: 0.8125rem;
    --space-2xs: 2px;
    --space-xs: 4px;
    --space-sm: 8px;
    --space-md: 16px;
    --space-lg: 24px;
    --space-xl: 32px;
    --space-2xl: 48px;
    --space-3xl: 64px;
    --space-4xl: 96px;
    --motion-fast: 160ms;
    --motion-base: 240ms;
    --motion-slow: 360ms;
    --ease-out: cubic-bezier(0.25, 0.1, 0.25, 1);
}}
:root[data-theme="dark"] {{
    color-scheme: dark;
    --black: #000000;
    --surface: #111111;
    --surface-raised: #1A1A1A;
    --border: #222222;
    --border-visible: #333333;
    --text-disabled: #666666;
    --text-secondary: #999999;
    --text-primary: #E8E8E8;
    --text-display: #FFFFFF;
    --interactive: var(--text-display);
    --home-title-size: 48px;
    --home-title-line-height: 50px;
    --home-title-weight: 700;
    --home-label-color: var(--text-secondary);
    --home-label-size: 12px;
}}
* {{
    box-sizing: border-box;
    letter-spacing: 0;
}}
body {{
    margin: 0;
    background: var(--black);
    color: var(--text-primary);
    font-family: "Space Grotesk", "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: var(--body);
    line-height: 1.5;
    transition: background-color var(--motion-base) var(--ease-out), color var(--motion-base) var(--ease-out);
}}
a {{
    color: inherit;
    text-decoration: none;
}}
.page {{
    max-width: 1200px;
    margin: 0 auto;
    padding: var(--space-4xl) var(--space-xl);
}}
.site-header {{
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    column-gap: var(--space-lg);
    row-gap: var(--space-xl);
    margin-bottom: calc(var(--space-xl) + var(--space-sm));
    padding-bottom: var(--space-lg);
    border-bottom: 0;
    opacity: 1;
}}
.eyebrow {{
    grid-column: 1 / 5;
    margin: 0;
    padding-left: 14px;
    color: var(--accent);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    font-weight: 700;
    line-height: 1.4;
    text-transform: uppercase;
}}
.eyebrow::before {{
    content: none;
}}
.header-title {{
    grid-column: 1 / 8;
    align-self: end;
}}
.header-label,
.metric-label,
.metric-meta,
.clock-label,
.clock-date {{
    margin: 0;
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    text-transform: uppercase;
    font-variant-numeric: tabular-nums;
}}
.header-label {{
    color: var(--home-label-color);
    font-size: var(--home-label-size);
}}
.header-label,
.metric-label,
.clock-label {{
    font-weight: 700;
}}
h1 {{
    margin: var(--space-sm) 0 0;
    max-width: 9em;
    color: var(--text-display);
    font-size: var(--home-title-size);
    line-height: var(--home-title-line-height);
    font-weight: var(--home-title-weight);
}}
.theme-toggle {{
    grid-column: 10 / 13;
    justify-self: end;
    align-self: start;
    min-height: 44px;
    padding: 12px 24px;
    background: transparent;
    border: 1px solid var(--border-visible);
    border-radius: 999px;
    color: var(--text-display);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--button);
    line-height: 1.4;
    font-weight: 700;
    text-transform: uppercase;
    cursor: pointer;
    transition: border-color var(--motion-fast) var(--ease-out), color var(--motion-fast) var(--ease-out), background-color var(--motion-fast) var(--ease-out);
}}
.theme-toggle:hover,
.theme-toggle:focus-visible {{
    border-color: var(--text-display);
}}
.header-instruments {{
    grid-column: 8 / 13;
    align-self: end;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: var(--space-lg);
}}
.metric-block {{
    min-height: 88px;
    padding-top: var(--space-md);
    border-top: 1px solid var(--border-visible);
    transition: border-color var(--motion-fast) var(--ease-out), opacity var(--motion-fast) var(--ease-out);
}}
.header-instruments > .metric-block:nth-child(-n + 2) {{
    border-top: 0;
}}
.metric-number,
.metric-value,
.clock-value {{
    color: var(--text-display);
    font-family: "Doto", "Space Mono", monospace;
    font-variant-numeric: tabular-nums;
}}
.metric-number {{
    display: block;
    margin-top: var(--space-sm);
    font-size: var(--display-lg);
    line-height: 0.9;
    font-weight: 700;
}}
.metric-value {{
    display: block;
    margin-top: var(--space-sm);
    font-size: var(--display-md);
    line-height: 0.95;
    font-weight: 700;
}}
.metric-meta {{
    margin-top: var(--space-sm);
}}
.home-clock {{
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: 1fr;
    row-gap: var(--space-sm);
    justify-items: start;
}}
.clock-value {{
    font-size: var(--display-lg);
    line-height: 0.95;
    font-weight: 700;
    transition: opacity var(--motion-fast) var(--ease-out), color var(--motion-fast) var(--ease-out);
}}
.clock-value.is-ticking {{
    opacity: 0.55;
}}
.clock-date {{
    padding-bottom: 4px;
}}
.archive-list {{
    border-top: 1px solid var(--border-visible);
}}
.archive-item {{
    display: grid;
    grid-template-columns: repeat(12, minmax(0, 1fr));
    column-gap: var(--space-lg);
    padding: var(--space-md) 0;
    border-bottom: 1px solid var(--border);
    transition: border-color var(--motion-fast) var(--ease-out), color var(--motion-fast) var(--ease-out);
}}
.item-index {{
    grid-column: 1 / 2;
    align-self: center;
    justify-self: center;
    text-align: center;
    color: var(--text-disabled);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 400;
    font-variant-numeric: tabular-nums;
}}
.date-block {{
    grid-column: 2 / 4;
    display: grid;
    grid-template-columns: 80px 1fr;
    gap: var(--space-md);
    align-items: center;
}}
.day {{
    color: var(--text-display);
    font-family: "Doto", "Space Mono", monospace;
    font-size: var(--display-lg);
    line-height: 0.9;
    font-weight: 700;
    letter-spacing: 0;
    font-variant-numeric: tabular-nums;
}}
.month {{
    color: var(--text-secondary);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    text-transform: uppercase;
}}
.entry-body {{
    grid-column: 4 / 13;
    display: grid;
    gap: var(--space-sm);
    align-content: center;
}}
.today-label {{
    display: inline-block;
    color: var(--accent);
    font-family: "Space Mono", "JetBrains Mono", "SF Mono", monospace;
    font-size: var(--caption);
    line-height: 1.4;
    font-weight: 700;
    text-transform: uppercase;
    width: fit-content;
    padding: 4px 12px;
    border: 1px solid rgba(255, 102, 31, 0.5);
    border-radius: 999px;
}}
.entry-title {{
    margin: 0;
    color: var(--text-primary);
    max-width: 65ch;
    font-size: var(--subheading);
    line-height: 1.3;
    font-weight: 400;
    text-decoration: underline;
    text-decoration-color: transparent;
    text-decoration-thickness: 1px;
    text-underline-offset: 4px;
    transition: color var(--motion-fast) var(--ease-out), text-decoration-color var(--motion-fast) var(--ease-out);
}}
.archive-item:hover .entry-title,
.archive-item:focus-visible .entry-title {{
    color: var(--text-display);
    text-decoration-color: currentColor;
}}
.archive-item:focus-visible {{
    outline: 2px solid var(--text-display);
    outline-offset: 4px;
}}
.empty {{
    margin: 0;
    padding: var(--space-xl) 0;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border);
}}
@media (prefers-reduced-motion: no-preference) {{
    @keyframes nd-power-on {{
        from {{ opacity: 0; }}
        to {{ opacity: 1; }}
    }}
    .site-header,
    .archive-list {{
        animation: nd-power-on var(--motion-base) var(--ease-out) both;
    }}
    .archive-list {{
        animation-delay: 80ms;
    }}
    .archive-item {{
        animation: nd-power-on var(--motion-slow) var(--ease-out) both;
        animation-delay: var(--row-delay, 120ms);
    }}
}}
@media (prefers-reduced-motion: reduce) {{
    *,
    *::before,
    *::after {{
        animation-duration: 1ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 1ms !important;
        scroll-behavior: auto !important;
    }}
}}
@media (max-width: 640px) {{
    .page {{
        padding: var(--space-3xl) var(--space-md);
    }}
    .site-header {{
        grid-template-columns: 1fr;
        gap: var(--space-lg);
        margin-bottom: var(--space-3xl);
        padding-bottom: var(--space-md);
    }}
    .eyebrow,
    .theme-toggle,
    .header-title,
    .header-instruments,
    .home-clock {{
        grid-column: 1;
    }}
    .theme-toggle {{
        justify-self: start;
    }}
    h1 {{
        font-size: var(--display-md);
    }}
    .header-instruments {{
        grid-template-columns: 1fr;
        gap: var(--space-md);
    }}
    .metric-block {{
        min-height: 0;
    }}
    .archive-item {{
        grid-template-columns: 1fr;
        gap: var(--space-md);
        padding: var(--space-md) 0;
    }}
    .item-index,
    .date-block {{
        grid-column: 1;
    }}
    .date-block {{
        grid-template-columns: 64px 1fr;
        gap: var(--space-md);
    }}
    .day {{
        font-size: var(--display-lg);
    }}
    .month {{
        font-size: var(--button);
    }}
    .entry-title {{
        font-size: var(--subheading);
    }}
    .entry-body {{
        grid-column: 1;
    }}
}}
</style>
<script src="https://mcp.figma.com/mcp/html-to-design/capture.js" async></script>
</head>
<body>
<main class="page">
    <header class="site-header">
        <p class="eyebrow">Archive</p>
        <button class="theme-toggle" type="button" data-theme-toggle>[ DARK ]</button>
        <div class="header-title">
            <p class="header-label">Daily News</p>
            <h1>The Babel Brief</h1>
        </div>
        <div class="header-instruments" aria-label="归档仪表">
            <div class="metric-block">
                <p class="metric-label">Latest</p>
                <span class="metric-number">{escape(latest_day)}</span>
                <p class="metric-meta">{escape(latest_date)}</p>
            </div>
            <div class="metric-block">
                <p class="metric-label">Total</p>
                <span class="metric-value">{escape(archive_count)}</span>
                <p class="metric-meta">Issues</p>
            </div>
            <div class="metric-block home-clock" aria-label="北京时间实时钟表">
                <p class="clock-label">CN Time</p>
                <time class="clock-value" data-cn-clock>{escape(clock_text)}</time>
                <p class="clock-date" data-cn-date>{escape(clock_date)}</p>
            </div>
        </div>
    </header>
    <section class="archive-list" aria-label="The Babel Brief archive">
        {rows}
    </section>
</main>
{DETAIL_CLOCK_SCRIPT}
</body>
</html>
"""


def render_archive_row(entry: ArchiveEntry, today: Date, index: int) -> str:
    archive_date = entry.archive_date.isoformat()
    month = f"{MONTH_NAMES[entry.archive_date.month]} {entry.archive_date.year}"
    today_label = '<span class="today-label">Today</span>' if entry.archive_date == today else ""
    row_delay = min(index, 10) * 35 + 120

    return f"""<a class="archive-item" href="/archives/{archive_date}" style="--row-delay: {row_delay}ms">
    <span class="item-index">{index:02d}</span>
    <div class="date-block" aria-label="{escape(archive_date)}">
        <span class="day">{entry.archive_date.day}</span>
        <span class="month">{escape(month)}</span>
    </div>
    <div class="entry-body">
        {today_label}
        <p class="entry-title">{escape(entry.title)}</p>
    </div>
</a>"""


def archive_path_for(date_text: str) -> Path:
    if not ARCHIVE_NAME_RE.fullmatch(date_text):
        raise HTTPException(status_code=404, detail="Archive not found")

    try:
        Date.fromisoformat(date_text)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Archive not found") from exc

    archive_path = ARCHIVE_DIR / f"{date_text}.html"
    if not archive_path.is_file():
        raise HTTPException(status_code=404, detail="Archive not found")

    return archive_path


def render_detail_header(date_text: str, title: str) -> str:
    archive_date = Date.fromisoformat(date_text)
    clock_text = datetime.now(LOCAL_TZ).strftime("%H:%M")
    return f"""
<header class="brief-detail-header">
    <div class="brief-detail-instruments">
        <p class="instrument-label">Issue</p>
        <p class="instrument-number">{archive_date.day:02d}</p>
        <p class="instrument-meta">{archive_date.month:02d} / {archive_date.year}</p>
        <p class="instrument-label">CN Time</p>
        <time class="instrument-clock" data-cn-clock>{escape(clock_text)}</time>
    </div>
    <h1 class="brief-detail-title">{escape(title)}</h1>
</header>
"""


def add_detail_chrome(html_content: str, date_text: str, title: str) -> str:
    return re.sub(
        r"(<body\b[^>]*>)",
        lambda match: f"{match.group(1)}\n{BACK_LINK_HTML}\n{render_detail_header(date_text, title)}\n{DETAIL_CLOCK_SCRIPT}",
        html_content,
        count=1,
        flags=re.IGNORECASE,
    ) if re.search(r"<body\b", html_content, flags=re.IGNORECASE) else (
        f"{BACK_LINK_HTML}\n{render_detail_header(date_text, title)}\n{DETAIL_CLOCK_SCRIPT}\n{html_content}"
    )


def add_detail_style(html_content: str) -> str:
    html_content = re.sub(r"<style\b[^>]*>.*?</style>", "", html_content, flags=re.IGNORECASE | re.DOTALL)
    if re.search(r"</head>", html_content, flags=re.IGNORECASE):
        return re.sub(
            r"</head>",
            f"{DETAIL_STYLE_HTML}\n<script src=\"https://mcp.figma.com/mcp/html-to-design/capture.js\" async></script>\n</head>",
            html_content,
            count=1,
            flags=re.IGNORECASE,
        )
    return f"{DETAIL_STYLE_HTML}\n<script src=\"https://mcp.figma.com/mcp/html-to-design/capture.js\" async></script>\n{html_content}"


def remove_decorative_markers(html_content: str) -> str:
    for marker in DECORATIVE_MARKERS:
        html_content = html_content.replace(marker, "")
    return html_content


def simplify_detail_headings(html_content: str) -> str:
    def clean_h2(match: re.Match[str]) -> str:
        heading = normalize_text(unescape(match.group(1)))
        heading = remove_decorative_markers(heading)
        heading = re.sub(r"^【(.+)】$", r"\1", heading)
        heading = DETAIL_HEADING_LABELS.get(heading, heading)
        return f"<h2>{heading}</h2>"

    return re.sub(r"<h2>(.*?)</h2>", clean_h2, html_content, flags=re.DOTALL)


def render_archive_detail_html(html_content: str, date_text: str) -> str:
    title = shorten_archive_title(extract_title(html_content), limit=64)
    html_content = remove_decorative_markers(html_content)
    html_content = simplify_detail_headings(html_content)
    html_content = add_detail_style(html_content)
    return add_detail_chrome(html_content, date_text, title)


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(render_index(read_archive_entries()))


@app.get("/archives/{date}", response_class=HTMLResponse)
def archive_detail(date: str) -> HTMLResponse:
    html_content = archive_path_for(date).read_text(encoding="utf-8", errors="replace")
    return HTMLResponse(render_archive_detail_html(html_content, date))
