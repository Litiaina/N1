#!/usr/bin/env python3
"""Finite proposal model, not a production N2 or filesystem qualification.

Run with Python's standard library only. All model state is held in memory.
"""

from collections import deque
from dataclasses import dataclass, replace
from functools import lru_cache
import json
import unittest


@dataclass(frozen=True)
class StageState:
    pointers: tuple = (True, True)
    owners: tuple = (0, 0)  # 0 = free, 1 = remover, 2 = healer
    remove_phase: int = 0  # acquire, write, acknowledge, release, done
    heal_phase: int = 0  # acquire, observe, write, release, done
    removed: int = 0
    repaired: int = 0
    observed: bool | None = None
    acknowledged: bool = False


def changed_tuple(values, index, value):
    return values[:index] + (value,) + values[index + 1:]


def successors(state, fresh_read=True):
    """Every next action for two finite attempts and two exact holders.

    Acquisition is ordered and fail-fast. Failed attempts release partial
    ownership; retries and unbounded liveness are outside this finite model.
    Writes are atomic model steps; active-I/O fencing is modeled below.
    """
    for actor, phase_name in ((1, "remove_phase"), (2, "heal_phase")):
        phase = getattr(state, phase_name)
        owned = [index for index, owner in enumerate(state.owners) if owner == actor]
        if phase == 0:
            missing = [index for index in range(2) if index not in owned]
            if not missing:
                yield f"{actor}:all-holders-owned", replace(state, **{phase_name: 1})
            else:
                target = missing[0]
                if state.owners[target] == 0:
                    yield f"{actor}:acquire-{target}", replace(
                        state, owners=changed_tuple(state.owners, target, actor)
                    )
                else:
                    yield f"{actor}:conflict-release-abort", replace(
                        state,
                        owners=tuple(0 if owner == actor else owner for owner in state.owners),
                        **{phase_name: 4},
                    )
        elif actor == 1 and phase == 1:
            if state.removed == 3:
                yield "remove:all-writes-joined", replace(state, remove_phase=2)
            else:
                for target in range(2):
                    if not state.removed & (1 << target):
                        yield f"remove:write-{target}", replace(
                            state,
                            pointers=changed_tuple(state.pointers, target, False),
                            removed=state.removed | (1 << target),
                        )
        elif actor == 1 and phase == 2:
            yield "remove:ACK", replace(state, acknowledged=True, remove_phase=3)
        elif actor == 2 and phase == 1:
            # Negative control deliberately reuses an old unlocked observation.
            observed = any(state.pointers) if fresh_read else True
            yield f"heal:fresh-source={observed}" if fresh_read else "heal:reuse-stale-source", replace(
                state, observed=observed, heal_phase=2 if observed else 3
            )
        elif actor == 2 and phase == 2:
            if state.repaired == 3:
                yield "heal:all-writes-joined", replace(state, heal_phase=3)
            else:
                for target in range(2):
                    if not state.repaired & (1 << target):
                        yield f"heal:write-{target}", replace(
                            state,
                            pointers=changed_tuple(state.pointers, target, True),
                            repaired=state.repaired | (1 << target),
                        )
        elif phase == 3:
            yield f"{actor}:release", replace(
                state,
                owners=tuple(0 if owner == actor else owner for owner in state.owners),
                **{phase_name: 4},
            )


def explore_stages(fresh_read=True):
    initial = StageState()
    parents = {initial: None}
    pending = deque([initial])
    transitions = 0
    violation = None
    terminal_states = 0
    while pending:
        state = pending.popleft()
        if state.acknowledged and any(state.pointers) and violation is None:
            trace = []
            current = state
            while parents[current] is not None:
                current, event = parents[current]
                trace.append(event)
            violation = list(reversed(trace))
        if state.remove_phase == state.heal_phase == 4:
            terminal_states += 1
        for event, following in successors(state, fresh_read):
            transitions += 1
            if following not in parents:
                parents[following] = (state, event)
                pending.append(following)

    @lru_cache(maxsize=None)
    def schedules(state):
        children = list(successors(state, fresh_read))
        return sum(schedules(child) for _, child in children) if children else 1

    return {
        "holder_count": 2,
        "attempt_count": 2,
        "exhaustive_reachable_states": len(parents),
        "exhaustive_transitions": transitions,
        "terminal_states": terminal_states,
        "finite_terminal_schedules": schedules(initial),
        "stale_resurrection_trace": violation,
    }


@dataclass(frozen=True)
class Grant:
    owner: str
    incarnation: int
    nonce: int
    expires: int


class Receiver:
    """Atomic per-key receiver state, with explicit admitted-write completion.

    Counters stand for unpredictable, never-reused process/grant identities.
    This is an abstraction, not a proposed token wire encoding.
    """

    def __init__(self):
        self.now = 0
        self.incarnation = 1
        self.nonce = 0
        self.leases = {}
        self.active = {}
        self.values = {}

    def acquire(self, key, owner, ttl=5):
        if ttl <= 0 or key in self.active:
            return None
        current = self.leases.get(key)
        if current is not None and current.expires > self.now:
            return None
        self.nonce += 1
        token = Grant(owner, self.incarnation, self.nonce, self.now + ttl)
        self.leases[key] = token
        return token

    def valid(self, key, token):
        return (
            token is not None
            and token.incarnation == self.incarnation
            and self.leases.get(key) == token
            and token.expires > self.now
        )

    def begin_write(self, key, token, value):
        if not self.valid(key, token) or key in self.active:
            return False
        self.active[key] = (token, value)
        return True

    def finish_write(self, key, token):
        active = self.active.get(key)
        if active is None or active[0] != token or token.incarnation != self.incarnation:
            return False
        # Expiry cannot revoke an already admitted filesystem operation while
        # allowing a replacement writer: acquire remains blocked until drain.
        self.values[key] = active[1]
        del self.active[key]
        return True

    def release(self, key, token):
        if key in self.active or self.leases.get(key) != token:
            return False
        del self.leases[key]
        return True

    def renew(self, key, token, ttl=5):
        if ttl <= 0 or not self.valid(key, token):
            return None
        renewed = replace(token, expires=self.now + ttl)
        self.leases[key] = renewed
        return renewed

    def restart(self):
        self.incarnation += 1
        self.nonce = 0
        self.leases.clear()
        self.active.clear()


class CoordinationModelTests(unittest.TestCase):
    def test_legacy_target_check_then_fresh_source_can_resurrect_after_remove_ack(self):
        pointers = [True, True]
        pointers[0] = False  # remove(T) completed, remove(S) still delayed
        target_observation = pointers[0]  # even a fresh target CAS sees absence
        fresh_source = pointers[1]  # old source still readable
        pointers[1] = False
        removal_acknowledged = not any(pointers)
        if pointers[0] == target_observation and fresh_source:
            pointers[0] = True
        self.assertTrue(removal_acknowledged)
        self.assertEqual(pointers, [True, False])

    def test_every_bounded_owned_stage_interleaving_preserves_acknowledged_removal(self):
        result = explore_stages()
        self.assertIsNone(result["stale_resurrection_trace"], result)
        self.assertGreater(result["exhaustive_reachable_states"], 20)
        self.assertGreater(result["finite_terminal_schedules"], 20)

    def test_negative_control_reusing_unlocked_source_fails_even_with_stage_ownership(self):
        result = explore_stages(fresh_read=False)
        self.assertIsNotNone(result["stale_resurrection_trace"])
        self.assertIn("remove:ACK", result["stale_resurrection_trace"])

    def test_expired_or_released_tokens_cannot_begin_delayed_writes(self):
        for end in ("expire", "release"):
            with self.subTest(end=end):
                receiver = Receiver()
                old = receiver.acquire("key", "old")
                if end == "expire":
                    receiver.now = old.expires
                else:
                    self.assertTrue(receiver.release("key", old))
                current = receiver.acquire("key", "new")
                self.assertIsNotNone(current)
                self.assertFalse(receiver.begin_write("key", old, "stale"))
                self.assertFalse(receiver.release("key", old))
                self.assertIsNone(receiver.renew("key", old))
                self.assertTrue(receiver.begin_write("key", current, "current"))
                self.assertTrue(receiver.finish_write("key", current))
                self.assertEqual(receiver.values["key"], "current")

    def test_restart_rejects_old_write_finish_renew_and_release(self):
        receiver = Receiver()
        old = receiver.acquire("key", "same-attempt")
        self.assertTrue(receiver.begin_write("key", old, "stale"))
        receiver.restart()
        current = receiver.acquire("key", "same-attempt")
        self.assertNotEqual(old, current)
        self.assertFalse(receiver.finish_write("key", old))
        self.assertFalse(receiver.begin_write("key", old, "stale"))
        self.assertFalse(receiver.release("key", old))
        self.assertIsNone(receiver.renew("key", old))
        self.assertTrue(receiver.begin_write("key", current, "new"))
        self.assertTrue(receiver.finish_write("key", current))
        self.assertEqual(receiver.values["key"], "new")

    def test_delayed_acquire_regrant_has_a_new_nonce_even_for_same_attempt(self):
        receiver = Receiver()
        old = receiver.acquire("key", "attempt")
        self.assertTrue(receiver.release("key", old))
        delayed_grant = receiver.acquire("key", "attempt")
        self.assertNotEqual(old.nonce, delayed_grant.nonce)
        self.assertFalse(receiver.begin_write("key", old, "stale"))
        receiver.now = delayed_grant.expires
        successor = receiver.acquire("key", "successor")
        self.assertFalse(receiver.begin_write("key", delayed_grant, "late"))
        self.assertTrue(receiver.begin_write("key", successor, "new"))

    def test_admitted_write_blocks_replacement_past_expiry_until_completion(self):
        receiver = Receiver()
        old = receiver.acquire("key", "old")
        self.assertTrue(receiver.begin_write("key", old, "old"))
        receiver.now = old.expires + 100
        self.assertIsNone(receiver.acquire("key", "replacement"))
        self.assertFalse(receiver.release("key", old))
        self.assertTrue(receiver.finish_write("key", old))
        current = receiver.acquire("key", "replacement")
        self.assertIsNotNone(current)
        self.assertFalse(receiver.begin_write("key", old, "stale"))
        self.assertTrue(receiver.begin_write("key", current, None))
        self.assertTrue(receiver.finish_write("key", current))
        self.assertIsNone(receiver.values["key"])

    def test_blocked_key_does_not_block_independent_key_progress(self):
        receiver = Receiver()
        old = receiver.acquire("blocked", "old")
        self.assertTrue(receiver.begin_write("blocked", old, "old"))
        receiver.now = old.expires
        self.assertIsNone(receiver.acquire("blocked", "replacement"))
        independent = receiver.acquire("independent", "other")
        self.assertIsNotNone(independent)
        self.assertFalse(receiver.begin_write("independent", old, "wrong-key"))
        self.assertTrue(receiver.begin_write("independent", independent, "new"))
        self.assertTrue(receiver.finish_write("independent", independent))
        self.assertEqual(receiver.values["independent"], "new")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CoordinationModelTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(json.dumps({
        "status": "PASS" if result.wasSuccessful() else "FAIL",
        "scope": "finite in-memory protocol proposal model; NOT production qualification",
        "tests_run": result.testsRun,
        "guarded_stage_exploration": explore_stages(),
        "stale_source_negative_control": explore_stages(fresh_read=False),
        "excluded": [
            "unbounded retries and fairness", "multiple pools", "membership transitions",
            "Byzantine peers or token cryptography", "filesystem crash/power durability",
            "real network or engine implementation", "compound rename atomicity",
        ],
    }, indent=2), flush=True)
    raise SystemExit(0 if result.wasSuccessful() else 1)
