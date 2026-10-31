"""Independent negative controls for literal embedded-delimiter clipboard output."""

import csv
import io

import pytest


def quoted_row(source, neighbor):
    output = io.StringIO(newline="")
    csv.writer(output, delimiter="\t", lineterminator="\n").writerow(["    " + source, neighbor])
    return output.getvalue().removesuffix("\n")


@pytest.mark.parametrize("source,neighbor", [("left\tright", "2"), ("first\nsecond", ""), ("first\r\nsecond", "3")])
def test_embedded_clipboard_preserves_exact_rectangle_and_os_line_endings(source, neighbor):
    from tests.flagship_embedded_clipboard import assert_embedded_clipboard

    text = quoted_row(source, neighbor)
    assert assert_embedded_clipboard(text, source, neighbor) == [["    " + source.replace("\r\n", "\n"), neighbor]]
    assert_embedded_clipboard(text.replace("\r\n", "\n").replace("\n", "\r\n"), source, neighbor)


@pytest.mark.parametrize("damage", ["unquoted", "wrong-prefix", "missing-neighbor", "wrong-neighbor", "extra-row", "extra-column"])
def test_embedded_clipboard_rejects_corruption(damage):
    from tests.flagship_embedded_clipboard import assert_embedded_clipboard

    text = quoted_row("left\tright", "2")
    if damage == "unquoted":
        text = text.replace('"', '')
    elif damage == "wrong-prefix":
        text = text.replace("    ", "bad ")
    elif damage == "missing-neighbor":
        text = text.removesuffix("\t2")
    elif damage == "wrong-neighbor":
        text = text.removesuffix("2") + "3"
    elif damage == "extra-row":
        text += "\nextra\t2"
    else:
        text += "\textra"
    with pytest.raises(AssertionError):
        assert_embedded_clipboard(text, "left\tright", "2")


def test_embedded_delimiters_have_installed_host_mappings():
    from pathlib import Path
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements

    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for kind in ("tabs", "newlines"):
        for host in ("dash", "react", "streamlit"):
            assert plan[(f"copy-selection.{kind}", host, f"browser.copy-selection.{kind}")] == (
                f"tests/test_flagship_workflows.py::test_flagship_embedded_clipboard[{kind}-{host}]")


@pytest.mark.parametrize("source", ["first\nsecond", "first\r\nsecond"])
@pytest.mark.parametrize("damage", ["unquoted", "collapsed", "extra-field"])
def test_embedded_newlines_reject_lost_breaks_and_changed_shape(source, damage):
    from tests.flagship_embedded_clipboard import assert_embedded_clipboard

    text = quoted_row(source, "3")
    if damage == "unquoted":
        text = text.replace('"', '')
    elif damage == "collapsed":
        text = text.replace("\r\n", "\n").replace("\n", " ")
    else:
        text += "\t3"
    with pytest.raises(AssertionError):
        assert_embedded_clipboard(text, source, "3")
