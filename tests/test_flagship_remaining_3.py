"""Chart evidence parser mutation tests; qualificationEvidence:false."""
import copy
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET
import pytest

from tests.flagship_remaining_3 import REQUIREMENTS, INITIAL_ROWS, parse_chart_csv, assert_chart_source, assert_svg_image

CSV = '\ufeffTitle,Chart Pane 1\nLegend,Ventes €,Unités\n\nCategory,Ventes €,Unités\nÉclair,100.25,2\nMünchen,49.75,3'.encode('utf-8')


def test_actual_chart_builder_preserves_explicit_rows_on_reselection():
    node = shutil.which("node")
    assert node
    result = subprocess.run([node, str(Path(__file__).with_name("frontend_flagship_charts.cjs"))], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "flagship chart runtime passed" in result.stdout


def test_chart_contract_has_fifteen_executable_host_nodes():
    from scripts.certify_flagship import _product_browser_plan
    from tests.flagship_contract import load_requirements
    plan = _product_browser_plan(load_requirements(Path(__file__).with_name("flagship_requirements.json")))
    for requirement in REQUIREMENTS:
        for host in ("dash", "react", "streamlit"):
            assert plan[(requirement, host, "browser." + requirement)] == f"tests/test_flagship_workflows.py::test_flagship_charts[{requirement}-{host}]"


def test_chart_source_checks_raw_precision_order_extras_and_series_identity():
    source = parse_chart_csv(CSV)
    assert_chart_source(source, INITIAL_ROWS)
    for damage in ("precision", "order", "excluded", "old-series"):
        bad = copy.deepcopy(source)
        if damage == "precision": bad["rows"][0][1] = 100
        elif damage == "order": bad["rows"].reverse()
        elif damage == "excluded": bad["rows"].append(["São Paulo", 900, 99])
        else: bad["header"] = ["Category", "Unités", "Ventes €"]
        with pytest.raises(AssertionError):
            assert_chart_source(bad, INITIAL_ROWS, source["header"])


@pytest.mark.parametrize("bad", [
    CSV.replace(b'100.25,2', b'100.25,2,12'),
    CSV.replace('Ventes €,Unités'.encode(), 'Unités,Ventes €'.encode()),
    CSV + '\nÉclair,100.25,2'.encode(),
    CSV.replace(b'Category', b'Missing'),
])
def test_chart_csv_rejects_invalid_model(bad):
    with pytest.raises(AssertionError):
        parse_chart_csv(bad)


def _image():
    marks = ''.join(f'<rect data-tip-cat="{row[0]}" data-tip-ser="{series}" x="1" y="2" width="10" height="20"/>' for row in INITIAL_ROWS for series in ('Ventes €', 'Unités'))
    text = ''.join(f'<text x="16" y="27" font-size="11">{label}</text>' for label in ('Chart Pane 1', 'Éclair', 'München', 'Ventes €', 'Unités'))
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="760" height="398" viewBox="0 0 760 398">{marks}{text}</svg>'.encode()


def test_svg_checks_valid_image_dimensions_categories_legends_and_bar_marks():
    live = {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}
    assert_svg_image(_image(), live, parse_chart_csv(CSV))
    for old, new in ((b'width="760"', b'width="759"'), (b'height="398"', b'height="399"'),
                     ('<text x="16" y="27" font-size="11">Éclair</text>'.encode(), b''), ('<text x="16" y="27" font-size="11">Unités</text>'.encode(), b''),
                     (b'<rect data-tip-cat', b'<circle data-tip-cat')):
        with pytest.raises(AssertionError):
            assert_svg_image(_image().replace(old, new), live, parse_chart_csv(CSV))


@pytest.mark.parametrize("target,attribute,value", [
    ("bars", "height", "0"), ("bars", "width", "0"),
    ("bars", "height", "-1"), ("bars", "height", "NaN"), ("bars", "width", "Infinity"),
    ("bars", "display", "none"), ("labels", "display", "none"),
    ("bars", "visibility", "hidden"), ("labels", "visibility", "collapse"),
    ("bars", "opacity", "0"), ("labels", "fill-opacity", "0"),
    ("bars", "fill", "none"), ("bars", "fill", "transparent"),
    ("labels", "fill", "#12345600"), ("bars", "fill", "rgba(10,20,30,0)"),
    ("labels", "font-size", "0"), ("bars", "x", "NaN"),
    ("labels", "x", "99999"), ("labels", "y", "NaN"),
    ("bars", "style", "display:none"), ("labels", "style", "fill:rgba(1,2,3,0)"),
    ("ancestor", "display", "none"), ("ancestor", "visibility", "hidden"),
    ("ancestor", "style", "opacity:0"), ("ancestor", "fill-opacity", "0"),
    ("ancestor", "fill", "transparent"), ("ancestor", "clip-path", "url(#empty)"),
    ("ancestor", "mask", "url(#empty)"), ("ancestor", "filter", "opacity(0)"),
    ("ancestor", "transform", "scale(0)"), ("ancestor", "transform", "translate(9999 9999)"),
])
def test_svg_rejects_invisible_selected_bars_and_labels(target, attribute, value):
    root = ET.fromstring(_image())
    if target == "ancestor":
        parent = ET.Element("{http://www.w3.org/2000/svg}g", {attribute: value})
        parent.extend(list(root))
        root[:] = [parent]
    else:
        for node in root:
            if (target == "bars" and "data-tip-cat" in node.attrib) or (target == "labels" and node.tag.endswith("text")):
                node.set(attribute, value)
    with pytest.raises(AssertionError):
        assert_svg_image(ET.tostring(root), {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}, parse_chart_csv(CSV))


def test_svg_does_not_count_nonpainted_title_metadata_as_category_label():
    content = _image().replace('<text x="16" y="27" font-size="11">Éclair</text>'.encode(),
                               '<text x="16" y="27"><title>Éclair</title></text>'.encode())
    with pytest.raises(AssertionError):
        assert_svg_image(content, {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}, parse_chart_csv(CSV))


def test_svg_accepts_native_translated_labels_with_title_metadata_and_visible_inherited_paint():
    root = ET.fromstring(_image())
    group = ET.SubElement(root, "{http://www.w3.org/2000/svg}g", {"transform": "translate(0 42) rotate(0)", "fill": "#2563eb", "opacity": "0.9"})
    for node in list(root)[:-1]:
        root.remove(node)
        group.append(node)
    for node in group:
        if node.tag.endswith("text"):
            label = node.text
            node.text = None
            title = ET.SubElement(node, "{http://www.w3.org/2000/svg}title")
            title.text, title.tail = label, label
    image = assert_svg_image(ET.tostring(root), {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}, parse_chart_csv(CSV))
    assert image["labels"] == ["Chart Pane 1", "Éclair", "München", "Ventes €", "Unités"]


def test_svg_rejects_stylesheet_hiding_instead_of_ignoring_css():
    content = _image().replace(b'<rect', b'<style>rect,text {display:none}</style><rect', 1)
    with pytest.raises(AssertionError):
        assert_svg_image(content, {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}, parse_chart_csv(CSV))


def test_svg_retains_unique_selected_bar_identities():
    root = ET.fromstring(_image())
    marks = [node for node in root if "data-tip-cat" in node.attrib]
    marks[0].set("data-tip-ser", marks[1].get("data-tip-ser"))
    with pytest.raises(AssertionError):
        assert_svg_image(ET.tostring(root), {"viewBox": [0, 0, 760, 320], "title": "Chart Pane 1", "legend": ["Ventes €", "Unités"]}, parse_chart_csv(CSV))
