import threading

from pivot_engine.runtime import SessionRequestGate


def test_viewport_sequence_rejects_stale_window_in_same_epoch():
    gate = SessionRequestGate()
    session = "s-1"

    assert gate.register_request(session, state_epoch=1, window_seq=10, abort_generation=1, intent="viewport")
    assert not gate.register_request(session, state_epoch=1, window_seq=10, abort_generation=1, intent="viewport")
    assert not gate.register_request(session, state_epoch=1, window_seq=9, abort_generation=1, intent="viewport")
    assert gate.register_request(session, state_epoch=1, window_seq=11, abort_generation=1, intent="viewport")


def test_new_epoch_supersedes_old_epoch():
    gate = SessionRequestGate()
    session = "s-2"

    assert gate.register_request(session, state_epoch=2, window_seq=5, abort_generation=1, intent="viewport")
    assert gate.response_is_current(session, state_epoch=2, window_seq=5, abort_generation=1, intent="viewport")

    assert gate.register_request(session, state_epoch=3, window_seq=1, abort_generation=2, intent="structural")
    assert not gate.response_is_current(session, state_epoch=2, window_seq=5, abort_generation=1, intent="viewport")
    assert gate.response_is_current(session, state_epoch=3, window_seq=1, abort_generation=2, intent="structural")


def test_abort_generation_rejects_older_requests():
    gate = SessionRequestGate()
    session = "s-3"

    assert gate.register_request(session, state_epoch=4, window_seq=2, abort_generation=4, intent="viewport")
    assert not gate.register_request(session, state_epoch=4, window_seq=3, abort_generation=3, intent="viewport")
    assert not gate.response_is_current(session, state_epoch=4, window_seq=2, abort_generation=3, intent="viewport")
    assert gate.response_is_current(session, state_epoch=4, window_seq=2, abort_generation=4, intent="viewport")


def test_structural_not_rejected_when_viewport_seq_advances_same_epoch():
    gate = SessionRequestGate()
    session = "s-4"

    assert gate.register_request(session, state_epoch=1, window_seq=10, abort_generation=1, intent="viewport")
    assert gate.register_request(session, state_epoch=1, window_seq=11, abort_generation=1, intent="viewport")
    assert gate.register_request(session, state_epoch=1, window_seq=10, abort_generation=1, intent="structural")
    assert gate.response_is_current(session, state_epoch=1, window_seq=10, abort_generation=1, intent="structural")


def test_client_instance_isolation_prevents_cross_mount_stale_poisoning():
    gate = SessionRequestGate()
    session = "s-5"

    assert gate.register_request(
        session, state_epoch=10, window_seq=100, abort_generation=10, intent="viewport", client_instance="old"
    )
    assert gate.register_request(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="structural", client_instance="new"
    )
    assert gate.response_is_current(
        session, state_epoch=1, window_seq=1, abort_generation=1, intent="structural", client_instance="new"
    )


def test_concurrent_registration_never_regresses_accepted_window_sequence():
    gate = SessionRequestGate()
    barrier = threading.Barrier(64)
    accepted = []
    accepted_lock = threading.Lock()

    def register(sequence):
        barrier.wait()
        result = gate.register_request(
            "concurrent",
            state_epoch=1,
            window_seq=sequence,
            abort_generation=0,
            intent="viewport",
        )
        with accepted_lock:
            accepted.append((sequence, result))

    threads = [threading.Thread(target=register, args=(sequence,)) for sequence in range(1, 65)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    highest_accepted = max(sequence for sequence, result in accepted if result)
    assert gate._read_state("concurrent")["window_seq"] == highest_accepted
