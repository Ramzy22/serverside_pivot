import threading
import time

from pivot_engine.execution_locks import TableExecutionLockRegistry


def test_table_execution_locks_are_scoped_and_report_wait_metrics():
    registry = TableExecutionLockRegistry()
    release = threading.Event()
    acquired = threading.Event()

    def hold_sales_lock():
        def operation():
            acquired.set()
            release.wait(timeout=2)
            return "held"

        registry.run("sales", operation)

    holder = threading.Thread(target=hold_sales_lock)
    holder.start()
    assert acquired.wait(timeout=1)

    result = []
    contender = threading.Thread(
        target=lambda: result.append(registry.run("sales", lambda: "next"))
    )
    contender.start()
    time.sleep(0.02)
    release.set()
    holder.join(timeout=1)
    contender.join(timeout=1)

    assert result == ["next"]
    sales = registry.snapshot("sales")
    assert sales["acquisitions"] == 2
    assert sales["waitMs"] > 0
    assert registry.snapshot("other")["acquisitions"] == 0
