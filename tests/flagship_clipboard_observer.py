"""Transparent clipboard-boundary observation, with native delivery preserved.

The wrappers forward the identical receiver/arguments synchronously and return
the identical native promise. They only read immutable ClipboardItem Blobs and
observe completion; no product serializer, event handler or data is replaced.
"""
import csv
import io
import time

CLIPBOARD_BOUNDARY_OBSERVER = r"""
(() => {
  if (window.__pivotClipboardBoundary) return;
  const events = window.__pivotClipboardBoundary = [];
  const clipboard = navigator.clipboard;
  if (!clipboard) return;
  for (const method of ['write', 'writeText']) {
    const native = clipboard[method];
    if (typeof native !== 'function') continue;
    Object.defineProperty(clipboard, method, {configurable:true, value:function(...args) {
      const event = {method, at:performance.now(), success:null, payloads:{}};
      events.push(event);
      // Forward before asynchronous observation: user activation and native
      // delivery receive the original values, with no await or substitution.
      let returned;
      try { returned = Reflect.apply(native, this, args); }
      catch (error) { event.success=false; event.error=String(error); throw error; }
      Promise.resolve(returned).then(() => {event.success=true;event.completedAt=performance.now();},
        error => {event.success=false;event.error=String(error);});
      if (method === 'writeText') {
        const text = String(args[0]);
        event.payloads['text/plain'] = {text, utf8:Array.from(new TextEncoder().encode(text))};
      } else {
        for (const item of args[0]) for (const type of item.types) {
          item.getType(type).then(blob => blob.text()).then(text => {
            event.payloads[type] = {text, utf8:Array.from(new TextEncoder().encode(text))};
          }, error => {event.captureError=String(error);});
        }
      }
      return returned;
    }});
  }
})();
"""


def assert_pre_delivery_rectangle(text, source, neighbor):
    """Before the OS boundary LF and CRLF must remain distinguishable."""
    expected = [["    " + source, neighbor]]
    output = io.StringIO(newline="")
    csv.writer(output, delimiter="\t", lineterminator="\n").writerows(expected)
    assert text == output.getvalue().removesuffix("\n"), "Product pre-delivery bytes differ from exact source"
    assert list(csv.reader(io.StringIO(text, newline=""), delimiter="\t")) == expected


def assert_single_clipboard_write(events, *, elapsed, requested):
    assert elapsed >= requested, "Clipboard writes were not monitored for the complete interval"
    assert len(events) == 1 and events[0].get("success") is True, "Shortcut must deliver exactly one successful native write"


def monitor_single_clipboard_write(driver, before, *, seconds):
    started = time.monotonic()
    # Poll the full interval so a late duplicate remains a failure. Reads never
    # write to the clipboard or replace a native/product event handler.
    events = []
    while time.monotonic() - started < seconds:
        events = driver.execute_script("return window.__pivotClipboardBoundary.slice(arguments[0])", before)
        assert len(events) <= 1, "Late duplicate clipboard write observed"
        time.sleep(min(.1, max(0, seconds - (time.monotonic() - started))))
    events = driver.execute_script("return window.__pivotClipboardBoundary.slice(arguments[0])", before)
    elapsed = time.monotonic() - started
    assert_single_clipboard_write(events, elapsed=elapsed, requested=seconds)
    return {"requestedSeconds": seconds, "elapsedSeconds": elapsed, "writes": events}
