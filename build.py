from pathlib import Path

from app import read_archive_entries, render_archive_detail_html, render_index


BASE_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = BASE_DIR / "archives"
DOCS_DIR = BASE_DIR / "docs"
DOCS_ARCHIVE_DIR = DOCS_DIR / "archives"


def main() -> None:
    DOCS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    entries = read_archive_entries()
    (DOCS_DIR / "index.html").write_text(render_index(entries), encoding="utf-8")

    archive_count = 0
    if ARCHIVE_DIR.exists():
        for archive_path in sorted(ARCHIVE_DIR.glob("*.html")):
            date_str = archive_path.stem
            content = archive_path.read_text(encoding="utf-8", errors="replace")
            rendered = render_archive_detail_html(content, date_str)
            (DOCS_ARCHIVE_DIR / archive_path.name).write_text(rendered, encoding="utf-8")
            archive_count += 1

    total_count = archive_count + 1
    print(f"Static site build complete: {total_count} files ({archive_count} archives, 1 index).")


if __name__ == "__main__":
    main()
