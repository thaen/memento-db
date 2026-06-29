from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from harness import SimClock
from lsmstore import LSMStore, make_factory

keys = st.binary(min_size=1, max_size=3)
vals = st.binary(min_size=0, max_size=5)


class LSMModel(RuleBasedStateMachine):
    """Fuzz the LSM store against a plain dict reference model. Hypothesis
    generates random put/delete/flush sequences and shrinks any failure to a
    minimal reproduction."""

    def __init__(self):
        super().__init__()
        self.disks = {}
        self.db = LSMStore(make_factory(self.disks), SimClock(), max_bytes=64)
        self.model = {}                      # the reference: a plain dict

    @rule(k=keys, v=vals)
    def put(self, k, v):
        self.db.put(k, v)
        self.model[k] = v

    @rule(k=keys)
    def delete(self, k):
        self.db.delete(k)
        self.model.pop(k, None)

    @rule()
    def flush(self):
        self.db.flush()

    @invariant()
    def matches_model(self):
        # Every key in the model reads back its model value; deleted/absent
        # keys read back None.
        for k, v in self.model.items():
            assert self.db.get(k) == v, (k, self.db.get(k), v)


TestLSM = LSMModel.TestCase
TestLSM.settings = settings(max_examples=200)
