from __future__ import annotations

import json

from finance_pi.docs_site import build_docs_site


def test_build_docs_site_publishes_all_markdown(tmp_path) -> None:
    (tmp_path / "README.md").write_text(
        "\n".join(
            [
                "# Project",
                "",
                "A **bold** note with `code`.",
                "",
                "| Name | Value |",
                "|---|---|",
                "| A | B |",
                "",
                "```bash",
                "echo hello",
                "```",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "runbook.md").write_text("# Runbook\n\n- one\n- two\n", encoding="utf-8")

    summary = build_docs_site(tmp_path)

    manifest = json.loads((summary.output_dir / "manifest.json").read_text(encoding="utf-8"))
    readme = (summary.output_dir / "readme.md.html").read_text(encoding="utf-8")
    assert summary.pages == 2
    assert summary.index_path.exists()
    assert len(manifest["pages"]) == 2
    assert "<table>" in readme
    assert "<pre><code" in readme


def test_prose_containing_pipes_is_not_treated_as_a_table(tmp_path) -> None:
    (tmp_path / "README.md").write_text(
        "\n".join(
            [
                "# Project",
                "",
                "Use `a | b` to pipe values, and note that x|y|z is not a table.",
                "",
                "| Name | Value |",
                "|---|---|",
                "| A | B |",
            ]
        ),
        encoding="utf-8",
    )

    summary = build_docs_site(tmp_path)

    readme = (summary.output_dir / "readme.md.html").read_text(encoding="utf-8")
    assert "<table>" in readme
    assert readme.count("<table>") == 1
    assert "<p>Use" in readme
    assert "not a table" in readme


def test_duplicate_headings_get_unique_anchor_ids(tmp_path) -> None:
    (tmp_path / "README.md").write_text(
        "\n".join(
            [
                "# Project",
                "",
                "## Overview",
                "",
                "text one",
                "",
                "## Overview",
                "",
                "text two",
                "",
                "## Overview",
                "",
                "text three",
            ]
        ),
        encoding="utf-8",
    )

    summary = build_docs_site(tmp_path)

    readme = (summary.output_dir / "readme.md.html").read_text(encoding="utf-8")
    assert 'id="overview"' in readme
    assert 'id="overview-2"' in readme
    assert 'id="overview-3"' in readme
    assert 'href="#overview"' in readme
    assert 'href="#overview-2"' in readme
    assert 'href="#overview-3"' in readme

