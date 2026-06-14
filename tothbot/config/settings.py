"""Run-mode configuration: the single paper/live flag.

TB00000 section 4.3 - PAPER IS THE MASTER. Paper trading is the
empirically-validated master; live mimics paper wherever physically
possible (so CIATS data stays valid for live). ONE set of documents and
ONE codebase govern both modes, selected by this single flag.

Paper and live differ at EXACTLY four physical-necessity points
(the PA-004 divergence points); everything else is byte-identical.
A NEW divergence point requires Bill's written authorization.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Mode(Enum):
    """The single mode selector. Paper is the master; live mimics paper."""

    PAPER = "paper"
    LIVE = "live"


# The four - and only four - paper/live physical-necessity divergence points
# (TB00000 section 4.3). Any addition requires Bill's written authorization.
DIVERGENCE_POINTS: tuple[str, ...] = (
    "private_ws_connection",  # 1: paper does not connect; live connects
    "order_dispatch",         # 2: paper simulated locally; live sent to Kraken
    "capital_ledger",         # 3: paper synthetic balance + identical fee math; live real Kraken balance
    "position_mirror_source", # 4: paper from simulated fills; live from Kraken executions
)


@dataclass(frozen=True)
class Settings:
    """Immutable run configuration. Defaults to PAPER (the master)."""

    mode: Mode = Mode.PAPER

    @property
    def is_paper(self) -> bool:
        return self.mode is Mode.PAPER

    @property
    def is_live(self) -> bool:
        return self.mode is Mode.LIVE
