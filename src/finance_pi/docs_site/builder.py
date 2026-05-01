# ruff: noqa: E501
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class DocsBuildSummary:
    output_dir: Path
    index_path: Path
    pages: int


@dataclass(frozen=True)
class DocPage:
    source: Path
    output_name: str
    title: str
    html: str
    headings: tuple[tuple[int, str, str], ...]


BlockMode = Literal["paragraph", "ul", "ol", "blockquote", "table", "code"]


DOCS_CSS = """
:root {
  color-scheme: light;
  --bg: #f5f7fa;
  --paper: #ffffff;
  --ink: #111827;
  --muted: #667085;
  --line: #d9dee7;
  --line-soft: #edf0f5;
  --rail: #151a22;
  --accent: #2563eb;
  --green: #0f766e;
  --code: #0f172a;
  --shadow: 0 18px 50px rgba(17, 24, 39, .08);
}
* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font: 16px/1.7 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
a:focus-visible { outline: 3px solid rgba(37, 99, 235, .24); outline-offset: 2px; border-radius: 6px; }
.layout { display: grid; grid-template-columns: 284px minmax(0, 1fr); min-height: 100vh; }
.sidebar {
  position: sticky;
  top: 0;
  height: 100vh;
  overflow: auto;
  padding: 22px 16px;
  background: var(--rail);
  color: white;
}
.brand { display: flex; align-items: center; gap: 12px; margin-bottom: 24px; padding: 0 6px; }
.mark {
  width: 42px;
  height: 42px;
  border-radius: 8px;
  display: grid;
  place-items: center;
  background: linear-gradient(135deg, var(--green), var(--accent));
  color: white;
  font-weight: 800;
  box-shadow: 0 10px 22px rgba(15, 118, 110, .28);
}
.brand strong, .brand span { display: block; }
.brand span { color: #aab3c2; font-size: 13px; }
.nav { display: grid; gap: 5px; }
.nav a {
  min-height: 38px;
  padding: 9px 10px;
  border-radius: 7px;
  color: #d8dee8;
  border: 1px solid transparent;
}
.nav a.active, .nav a:hover {
  color: white;
  background: #202733;
  border-color: rgba(255,255,255,.08);
  text-decoration: none;
}
.content {
  padding: 30px;
  min-width: 0;
  max-width: 1240px;
  width: 100%;
}
.doc {
  max-width: 980px;
  background: var(--paper);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 36px;
  box-shadow: var(--shadow);
}
.doc-home { max-width: 1080px; }
.doc-topline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 22px;
}
.breadcrumb, .meta, .footer { color: var(--muted); font-size: 13px; }
.meta { margin: 0 0 22px; }
h1, h2, h3, h4, h5, h6 { line-height: 1.24; letter-spacing: 0; scroll-margin-top: 20px; }
h1 { font-size: clamp(32px, 5vw, 48px); margin: 0 0 10px; }
h2 { font-size: 25px; margin-top: 38px; padding-top: 20px; border-top: 1px solid var(--line-soft); }
h3 { font-size: 20px; margin-top: 30px; }
h4, h5, h6 { margin-top: 24px; }
p { margin: 14px 0; }
ul, ol { padding-left: 24px; }
li { margin: 6px 0; }
code {
  background: #eef2f7;
  color: #182230;
  padding: 2px 5px;
  border-radius: 5px;
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
  font-size: .92em;
}
pre {
  overflow: auto;
  background: var(--code);
  color: #e5e7eb;
  padding: 16px;
  border-radius: 8px;
  line-height: 1.48;
  border: 1px solid #1e293b;
}
pre code { background: transparent; padding: 0; color: inherit; }
blockquote {
  margin: 18px 0;
  padding: 12px 16px;
  border-left: 4px solid var(--green);
  background: #f0fdfa;
  color: #344054;
}
table {
  border-collapse: collapse;
  width: 100%;
  display: block;
  overflow-x: auto;
  margin: 18px 0;
  border: 1px solid var(--line);
  border-radius: 8px;
}
th, td {
  border-bottom: 1px solid var(--line-soft);
  padding: 9px 11px;
  text-align: left;
  vertical-align: top;
}
th { background: #f8fafc; color: #475467; font-size: 13px; }
tr:last-child td { border-bottom: 0; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; margin-top: 22px; }
.card {
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 16px;
  background: #ffffff;
  display: grid;
  gap: 8px;
  min-height: 122px;
  box-shadow: 0 10px 24px rgba(17,24,39,.05);
}
.card:hover {
  border-color: #b6c0cf;
  text-decoration: none;
  transform: translateY(-1px);
}
.card strong { color: var(--ink); font-size: 17px; }
.card span { color: var(--muted); font-size: 13px; }
.toc {
  margin: 22px 0;
  padding: 14px;
  background: #f8fafc;
  border: 1px solid var(--line);
  border-radius: 8px;
}
.toc strong { display: block; margin-bottom: 7px; }
.toc a { display: block; padding: 4px 0; color: #344054; }
.toc .h3 { padding-left: 14px; font-size: 14px; }
.footer { margin-top: 30px; padding-top: 18px; border-top: 1px solid var(--line-soft); }
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition: none !important; }
}
@media (max-width: 900px) {
  .layout { grid-template-columns: 1fr; }
  .sidebar { position: static; height: auto; }
  .nav { grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }
  .content { padding: 16px; }
  .doc { padding: 22px; }
  .doc-topline { align-items: flex-start; flex-direction: column; }
}
"""


def build_docs_site(root: Path, output_dir: Path | None = None) -> DocsBuildSummary:
    root = root.resolve()
    output = output_dir or root / "data" / "docs_site"
    output.mkdir(parents=True, exist_ok=True)
    pages = [_build_page(path, root) for path in _discover_markdown(root)]
    for page in pages:
        (output / page.output_name).write_text(
            _page_html(page, pages, root),
            encoding="utf-8",
        )
    index_path = output / "index.html"
    index_path.write_text(_index_html(pages, root), encoding="utf-8")
    (output / "styles.css").write_text(DOCS_CSS, encoding="utf-8")
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "pages": [
                    {
                        "title": page.title,
                        "source": _relative_source(page.source, root),
                        "output": page.output_name,
                    }
                    for page in pages
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return DocsBuildSummary(output, index_path, len(pages))


def _discover_markdown(root: Path) -> list[Path]:
    docs: list[Path] = []
    for name in ["README.md", "finance-pi-architecture.md"]:
        path = root / name
        if path.exists():
            docs.append(path)
    docs_dir = root / "docs"
    if docs_dir.exists():
        docs.extend(sorted(docs_dir.rglob("*.md")))
    return docs


def _build_page(path: Path, root: Path) -> DocPage:
    source = path.read_text(encoding="utf-8")
    title = _title_for(source, path)
    body, headings = _markdown_to_html(source)
    return DocPage(path, f"{_slug(_relative_source(path, root))}.html", title, body, headings)


def _title_for(source: str, path: Path) -> str:
    for line in source.splitlines():
        match = re.match(r"^#\s+(.+)$", line)
        if match:
            return _strip_inline(match.group(1))
    return path.stem.replace("-", " ").replace("_", " ").title()


def _markdown_to_html(source: str) -> tuple[str, tuple[tuple[int, str, str], ...]]:
    lines = source.splitlines()
    html_parts: list[str] = []
    headings: list[tuple[int, str, str]] = []
    paragraph: list[str] = []
    list_mode: BlockMode | None = None
    blockquote: list[str] = []
    table: list[str] = []
    code: list[str] = []
    in_code = False
    code_lang = ""

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            html_parts.append(f"<p>{_inline(' '.join(paragraph))}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_mode
        if list_mode in {"ul", "ol"}:
            html_parts.append(f"</{list_mode}>")
            list_mode = None

    def flush_blockquote() -> None:
        nonlocal blockquote
        if blockquote:
            html_parts.append(f"<blockquote>{''.join(f'<p>{_inline(line)}</p>' for line in blockquote)}</blockquote>")
            blockquote = []

    def flush_table() -> None:
        nonlocal table
        if table:
            html_parts.append(_table_html(table))
            table = []

    def flush_all() -> None:
        flush_paragraph()
        flush_list()
        flush_blockquote()
        flush_table()

    for raw in lines:
        line = raw.rstrip()
        fence = re.match(r"^```(.*)$", line)
        if fence:
            if in_code:
                html_parts.append(
                    f'<pre><code class="language-{escape(code_lang)}">{escape(chr(10).join(code))}</code></pre>'
                )
                code = []
                code_lang = ""
                in_code = False
            else:
                flush_all()
                in_code = True
                code_lang = fence.group(1).strip()
            continue
        if in_code:
            code.append(line)
            continue
        if not line.strip():
            flush_all()
            continue
        heading = re.match(r"^(#{1,6})\s+(.+)$", line)
        if heading:
            flush_all()
            level = len(heading.group(1))
            text = _strip_inline(heading.group(2))
            anchor = _slug(text)
            headings.append((level, text, anchor))
            html_parts.append(f'<h{level} id="{anchor}">{_inline(text)}</h{level}>')
            continue
        if line.startswith(">"):
            flush_paragraph()
            flush_list()
            flush_table()
            blockquote.append(line.lstrip("> "))
            continue
        if _is_table_line(line):
            flush_paragraph()
            flush_list()
            flush_blockquote()
            table.append(line)
            continue
        unordered = re.match(r"^\s*[-*]\s+(.+)$", line)
        ordered = re.match(r"^\s*\d+\.\s+(.+)$", line)
        if unordered or ordered:
            flush_paragraph()
            flush_blockquote()
            flush_table()
            mode: BlockMode = "ul" if unordered else "ol"
            if list_mode != mode:
                flush_list()
                html_parts.append(f"<{mode}>")
                list_mode = mode
            item = unordered.group(1) if unordered else ordered.group(1)
            html_parts.append(f"<li>{_inline(item)}</li>")
            continue
        flush_list()
        flush_blockquote()
        flush_table()
        paragraph.append(line)
    flush_all()
    if in_code:
        html_parts.append(f"<pre><code>{escape(chr(10).join(code))}</code></pre>")
    return "\n".join(html_parts), tuple(headings)


def _table_html(lines: list[str]) -> str:
    rows = [_split_table(line) for line in lines if not _is_table_separator(line)]
    if not rows:
        return ""
    head, *body = rows
    head_html = "".join(f"<th>{_inline(cell)}</th>" for cell in head)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{_inline(cell)}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"


def _is_table_line(line: str) -> bool:
    return line.count("|") >= 2


def _is_table_separator(line: str) -> bool:
    cells = _split_table(line)
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def _split_table(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _inline(value: str) -> str:
    text = escape(value)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


def _strip_inline(value: str) -> str:
    return re.sub(r"[`*_]", "", value).strip()


def _index_html(pages: list[DocPage], root: Path) -> str:
    cards = "\n".join(
        f'<a class="card" href="{page.output_name}"><strong>{escape(page.title)}</strong>'
        f"<span>{escape(_relative_source(page.source, root))}</span></a>"
        for page in pages
    )
    content = (
        '<div class="doc-topline"><span class="breadcrumb">finance-pi docs</span>'
        f'<span class="meta">Generated {datetime.now(UTC).isoformat()}</span></div>'
        "<h1>finance-pi Documentation</h1>"
        f'<div class="cards">{cards}</div>'
    )
    return _shell("Documentation", content, pages, "index.html")


def _page_html(page: DocPage, pages: list[DocPage], root: Path) -> str:
    toc = _toc_html(page.headings)
    meta = f'<p class="meta">Source: <code>{escape(_relative_source(page.source, root))}</code></p>'
    return _shell(page.title, f"<h1>{escape(page.title)}</h1>{meta}{toc}{page.html}", pages, page.output_name)


def _shell(title: str, content: str, pages: list[DocPage], active: str) -> str:
    nav = "\n".join(
        f'<a class="{"active" if page.output_name == active else ""}" href="{page.output_name}">{escape(page.title)}</a>'
        for page in pages
    )
    article_class = "doc doc-home" if active == "index.html" else "doc"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)} - finance-pi docs</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand"><span class="mark">fp</span><div><strong>finance-pi</strong><span>Documentation</span></div></div>
      <nav class="nav"><a class="{"active" if active == "index.html" else ""}" href="index.html">Home</a>{nav}</nav>
    </aside>
    <main class="content"><article class="{article_class}">{content}<p class="footer">Published by finance-pi docs build.</p></article></main>
  </div>
</body>
</html>
"""


def _toc_html(headings: tuple[tuple[int, str, str], ...]) -> str:
    items = [
        f'<a class="h{level}" href="#{anchor}">{escape(text)}</a>'
        for level, text, anchor in headings
        if level in {2, 3}
    ]
    if not items:
        return ""
    return f'<div class="toc"><strong>Contents</strong>{"".join(items)}</div>'


def _relative_source(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _slug(value: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z가-힣._-]+", "-", value.strip().lower()).strip("-")
    return slug or "document"
