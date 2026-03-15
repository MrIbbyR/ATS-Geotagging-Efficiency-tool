# semantic_tagger.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import numpy as np

from sentence_transformers import SentenceTransformer

@dataclass(frozen=True)
class TagSpec:
    tag: str
    # A short description / “what this tag means”
    description: str
    # Optional phrases/synonyms that should imply this tag
    synonyms: Tuple[str, ...] = ()

def _device_auto() -> str:
    try:
        import torch
        if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
            return "mps"
        return "cpu"
    except Exception:
        return "cpu"

def _normalize_text(s: str) -> str:
    s = (s or "").strip()
    # Keep it simple; your scraper already has heavier normalization elsewhere.
    return " ".join(s.split())

def _cosine_sim_matrix(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    # A: (n, d), B: (m, d)
    A = A / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-12)
    B = B / (np.linalg.norm(B, axis=1, keepdims=True) + 1e-12)
    return A @ B.T

class SemanticTagger:
    """
    Embedding-based semantic tagger.
    - Build once, reuse for all profiles in the run.
    - Encodes each tag spec into an embedding.
    - Encodes each profile blob into an embedding.
    - Scores cosine similarity, returns tags above threshold.
    """

    def __init__(
        self,
        tag_specs: List[TagSpec],
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        device: Optional[str] = None,
    ):
        self.tag_specs = tag_specs
        self.device = device or _device_auto()
        self.model = SentenceTransformer(model_name, device=self.device)

        # Build one “prompt” per tag: description + synonyms
        self._tag_texts: List[str] = []
        self._tags: List[str] = []
        for spec in self.tag_specs:
            parts = [spec.tag, spec.description]
            if spec.synonyms:
                parts.append("Synonyms: " + ", ".join(spec.synonyms))
            self._tag_texts.append(_normalize_text(" | ".join(parts)))
            self._tags.append(spec.tag)

        self._tag_emb: np.ndarray = self.model.encode(
            self._tag_texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )

    def score(self, profile_text: str) -> List[Tuple[str, float]]:
        text = _normalize_text(profile_text)
        if not text:
            return []
        emb = self.model.encode(
            [text],
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )  # (1, d)
        sims = (emb @ self._tag_emb.T).reshape(-1)  # cosine, since normalized
        out = list(zip(self._tags, sims.tolist()))
        out.sort(key=lambda x: x[1], reverse=True)
        return out

    def predict(
        self,
        profile_text: str,
        threshold: float = 0.38,
        top_k: int = 6,
    ) -> List[Tuple[str, float]]:
        """
        Returns up to top_k tags above threshold.
        Typical thresholds:
          - 0.32–0.36 = more recall (more tags)
          - 0.37–0.42 = balanced
          - 0.43+     = strict (fewer tags, higher precision)
        """
        scored = self.score(profile_text)
        picked = [(t, s) for (t, s) in scored[:top_k] if s >= threshold]
        return picked
