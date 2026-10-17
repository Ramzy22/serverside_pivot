"""Product-native display order and clipboard literals are independent oracles."""

import pytest

from tests.flagship_browser_support import (
    _product_display_order_copy_oracle,
    assert_selected_clipboard,
)


def test_product_display_order_copy_uses_exact_native_order_and_literal_body():
    body = _product_display_order_copy_oracle([
        {"label": "Éclair"}, {"label": "München"},
    ])
    assert body == [["2.00", "100.25"], ["3.00", "49.75"]]
    assert_selected_clipboard("2.00\t100.25\n3.00\t49.75", body)
    with pytest.raises(AssertionError, match="clipboard bytes"):
        assert_selected_clipboard("3.00\t49.75\n2.00\t100.25", body)


@pytest.mark.parametrize("labels", [
    ["München", "Éclair"],
    ["Éclair", "Éclair"],
    ["Éclair"],
    ["Éclair", "München", "São Paulo"],
])
def test_product_display_order_copy_rejects_wrong_visible_order(labels):
    with pytest.raises(AssertionError, match="backend-native fixture oracle"):
        _product_display_order_copy_oracle([{"label": label} for label in labels])
