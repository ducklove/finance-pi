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

