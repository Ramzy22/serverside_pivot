"""Tooling-only observer/oracle tests; these are never release evidence."""
import json
import subprocess

import pytest


def test_observer_forwards_original_arguments_receiver_promise_and_blob_bytes():
    from tests.flagship_clipboard_observer import CLIPBOARD_BOUNDARY_OBSERVER
    script = r'''
const vm = require('node:vm');
const calls=[]; const promise=Promise.resolve();
const clipboard={write(items){calls.push({receiver:this,items});return promise;},writeText(text){return promise;}};
const scope={navigator:{clipboard},performance,TextEncoder,window:null};scope.window=scope;
vm.runInNewContext(process.argv[1],scope);
const blob=new Blob(['first\r\nsecond']);const items=[{types:['text/plain'],getType:()=>Promise.resolve(blob)}];
const returned=clipboard.write(items);
setTimeout(()=>process.stdout.write(JSON.stringify({samePromise:returned===promise,sameItems:calls[0].items===items,
sameReceiver:calls[0].receiver===clipboard,events:scope.__pivotClipboardBoundary})),20);
'''
    output = subprocess.run(["node", "-e", script, CLIPBOARD_BOUNDARY_OBSERVER], check=True, capture_output=True, text=True)
    data = json.loads(output.stdout)
    assert data["samePromise"] and data["sameItems"] and data["sameReceiver"]
    assert data["events"][0]["success"] is True
    assert data["events"][0]["payloads"]["text/plain"]["text"] == "first\r\nsecond"
    assert data["events"][0]["payloads"]["text/plain"]["utf8"] == list(b"first\r\nsecond")


@pytest.mark.parametrize("source", ["left\tright", "first\nsecond", "first\r\nsecond"])
def test_pre_delivery_oracle_preserves_exact_control_bytes(source):
    from tests.flagship_clipboard_observer import assert_pre_delivery_rectangle
    from tests.test_flagship_embedded_clipboard import quoted_row
    text = quoted_row(source, "3")
    assert_pre_delivery_rectangle(text, source, "3")
    if "\r\n" in source:
        with pytest.raises(AssertionError):
            assert_pre_delivery_rectangle(text.replace("\r\n", "\n"), source, "3")


@pytest.mark.parametrize("events", [[], [{"success": False}], [{"success": True}, {"success": True}]])
def test_shortcut_monitor_rejects_zero_failed_or_duplicate_writes(events):
    from tests.flagship_clipboard_observer import assert_single_clipboard_write
    with pytest.raises(AssertionError):
        assert_single_clipboard_write(events, elapsed=8.1, requested=8)


def test_shortcut_monitor_requires_complete_interval():
    from tests.flagship_clipboard_observer import assert_single_clipboard_write
    assert_single_clipboard_write([{"success": True}], elapsed=8.1, requested=8)
    with pytest.raises(AssertionError):
        assert_single_clipboard_write([{"success": True}], elapsed=0.1, requested=8)
