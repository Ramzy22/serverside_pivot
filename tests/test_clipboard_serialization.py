"""Run the shared clipboard serializer against literal TSV regression cases."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_shared_clipboard_serialization_preserves_cell_text():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the frontend source regression")

    script = Path(__file__).with_name("frontend_clipboard_serialization.cjs")
    result = subprocess.run(
        [node, "--test", str(script)],
        cwd=script.resolve().parents[1],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture(scope="module")
def html_fallback_browser():
    from selenium import webdriver

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    try:
        yield driver
    finally:
        driver.quit()


def _html_fallback_script():
    source_path = Path(__file__).resolve().parents[1] / "dash_tanstack_pivot/src/lib/utils/exportUtils.js"
    source = source_path.read_text(encoding="utf-8")
    functions = []
    for name in ("escapeTsv", "htmlTableToTsv"):
        start = source.index(f"export function {name}(")
        end = source.index("\nexport function ", start + 1)
        functions.append(source[start:end].replace("export ", "", 1))
    return "\n".join(functions) + "\nreturn htmlTableToTsv(arguments[0]);"


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        ('<table><tr><th>Before</th><th>Head\r\nline</th><th>After</th></tr>'
         '<tr><td>left</td><td>first\r\nsecond</td><td>right</td></tr></table>',
         'Before\t"Head\r\nline"\tAfter\nleft\t"first\r\nsecond"\tright'),
        ('<table><tr><td>left</td><td>first\rsecond</td><td>right</td></tr></table>',
         'left\t"first\rsecond"\tright'),
        ('<table><tr><td>left</td><td>first\nsecond\t&quot;東京&quot;</td><td></td></tr></table>',
         'left\t"first\nsecond\t""東京"""\t'),
        ('<table><tr><td>left</td><td>first&#13;second</td><td>&amp;#13;</td></tr></table>',
         'left\t"first\rsecond"\t&#13;'),
        ('<table><tr><td\rdata-note="quoted > boundary\rvalue"\rtitle="x">first\rsecond</td>'
         '<td data-note="&amp;#13;">ordinary</td></tr></table>',
         '"first\rsecond"\tordinary'),
        ('<table>\r\n<tr><td>left</td><td><!-- keep > comment -->first\rsecond</td><td>right</td></tr></table>',
         'left\t"first\rsecond"\tright'),
    ],
    ids=["crlf-header-and-cell", "lone-cr", "lf-tab-quotes-unicode-empty", "existing-and-literal-entity",
         "quoted-and-separated-attributes", "markup-whitespace-and-comment"],
)
def test_html_clipboard_fallback_preserves_cell_delimiters(html_fallback_browser, html, expected):
    assert html_fallback_browser.execute_script(_html_fallback_script(), html) == expected
