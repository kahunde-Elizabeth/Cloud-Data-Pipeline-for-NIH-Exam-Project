"""Load shapes from Annex A3 (Standard Test Profile). Rates are national figures / k."""
from __future__ import annotations

from dataclasses import dataclass

E1_RATE = 611.0                    # national mean, records/s
E2_PLATEAU = 815.0                 # national campaign plateau, records/s
E2_RAMP_S, E2_PLATEAU_S, E2_DECAY_S = 300, 900, 600     # 5 + 15 + 10 minutes
STEP_S = 300                       # step-stress: 5 minutes at 2x, then 5 minutes at 3x


@dataclass(frozen=True)
class Profile:
    name: str
    duration_s: float
    description: str

    def rate(self, t: float, k: int) -> float:          # records/s at second t
        raise NotImplementedError


@dataclass(frozen=True)
class Steady(Profile):
    national_rate: float = E1_RATE

    def rate(self, t, k):
        return self.national_rate / k if 0 <= t < self.duration_s else 0.0


@dataclass(frozen=True)
class Campaign(Profile):
    """E2: ramp 0 -> plateau over 5 min, plateau 15 min, decay to 0 over 10 min
    (about 1.1 million / k records), then optionally 2x and 3x the plateau, 5 min each."""
    step_stress: bool = False

    def rate(self, t, k):
        p = E2_PLATEAU / k
        if t < 0:
            return 0.0
        if t < E2_RAMP_S:
            return p * t / E2_RAMP_S
        t -= E2_RAMP_S
        if t < E2_PLATEAU_S:
            return p
        t -= E2_PLATEAU_S
        if t < E2_DECAY_S:
            return p * (1 - t / E2_DECAY_S)
        t -= E2_DECAY_S
        if self.step_stress and t < 2 * STEP_S:
            return p * (2 if t < STEP_S else 3)
        return 0.0


def get(name: str, minutes: float | None = None, rate: float | None = None) -> Profile:
    """e1: 10 min steady; e2: campaign burst; e2-step: burst + step-stress;
    steady: any length / rate (E4 faults, E5 downtime, the supervised session); e6: like e1."""
    if name in ("e1", "e6"):
        return Steady(name, 60 * (minutes or 10), "steady 611/k records/s",
                      national_rate=rate or E1_RATE)
    if name == "steady":
        return Steady(name, 60 * (minutes or 10), "steady load", national_rate=rate or E1_RATE)
    if name == "e2":
        return Campaign(name, E2_RAMP_S + E2_PLATEAU_S + E2_DECAY_S, "campaign burst")
    if name == "e2-step":
        return Campaign(name, E2_RAMP_S + E2_PLATEAU_S + E2_DECAY_S + 2 * STEP_S,
                        "campaign burst + step-stress", step_stress=True)
    raise ValueError(f"unknown profile {name!r}")


def total_records(profile: Profile, k: int, step: float = 1.0) -> float:
    t, total = 0.0, 0.0
    while t < profile.duration_s:
        total += profile.rate(t + step / 2, k) * step
        t += step
    return total
