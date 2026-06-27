"""
Modulo de distribuciones para el generador de trafico.
Implementa selectores Zipf y Uniforme, y un modelo de llegadas Poisson.
"""
import numpy as np
from typing import Sequence


class ZipfSelector:
    """Selecciona items siguiendo una distribucion Zipf (ley de potencia)."""

    def __init__(self, items: Sequence, s: float = 1.2, seed: int = 0):
        self.items = list(items)
        self.s = s
        self.rng = np.random.default_rng(seed)
        n = len(items)
        ranks = np.arange(1, n + 1)
        weights = 1.0 / np.power(ranks, s)
        self.probs = weights / weights.sum()

    def sample(self):
        idx = self.rng.choice(len(self.items), p=self.probs)
        return self.items[idx]

    def describe(self) -> dict:
        return {"distribution": "zipf", "s": self.s,
                "probs": [round(float(p), 4) for p in self.probs]}


class UniformSelector:
    """Selecciona items con probabilidad uniforme."""

    def __init__(self, items: Sequence, seed: int = 0):
        self.items = list(items)
        self.rng = np.random.default_rng(seed)

    def sample(self):
        idx = self.rng.integers(0, len(self.items))
        return self.items[idx]

    def describe(self) -> dict:
        return {"distribution": "uniform", "n": len(self.items)}


class PoissonInterArrival:
    """Modelo de llegadas Poisson para controlar la tasa de consultas."""

    def __init__(self, rate_qps: float, seed: int = 0):
        self.rate = rate_qps
        self.rng = np.random.default_rng(seed)

    def next_wait(self) -> float:
        return float(self.rng.exponential(1.0 / self.rate))


def build_selector(kind: str, items: Sequence, **kwargs):
    """Factory para crear el selector apropiado."""
    kind = kind.lower()
    if kind == "zipf":
        return ZipfSelector(items, s=kwargs.get("s", 1.2),
                            seed=kwargs.get("seed", 0))
    if kind == "uniform":
        return UniformSelector(items, seed=kwargs.get("seed", 0))
    raise ValueError(f"Distribucion desconocida: {kind}")
