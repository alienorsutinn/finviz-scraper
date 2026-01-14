"""
Model Registry for tracking and versioning trained ML models.

Provides:
- Model versioning with semantic versioning
- Metadata tracking (training date, features, performance metrics)
- Model comparison and selection
- Production model deployment
- Model archival and cleanup
"""
from __future__ import annotations

import json
import logging
import pickle
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

LOGGER = logging.getLogger(__name__)

DEFAULT_REGISTRY_PATH = Path("models/registry")


@dataclass
class ModelMetadata:
    """Metadata for a trained model."""

    model_id: str  # Unique identifier (e.g., "xgboost_v1_20240115")
    model_type: str  # Model type (xgboost, ensemble, etc.)
    version: str  # Semantic version (e.g., "1.0.0")

    # Training information
    trained_at: str  # ISO format timestamp
    training_start_date: str  # Data range start
    training_end_date: str  # Data range end
    training_samples: int  # Number of training samples

    # Feature information
    feature_names: List[str]
    feature_count: int

    # Performance metrics
    train_ic: float  # In-sample information coefficient
    val_ic: float  # Validation information coefficient
    oos_ic: Optional[float] = None  # Out-of-sample IC (from walk-forward)
    train_r2: float = 0.0
    val_r2: float = 0.0
    oos_sharpe: Optional[float] = None

    # Model configuration
    hyperparameters: Dict[str, Any] = field(default_factory=dict)

    # Deployment status
    is_production: bool = False
    deployed_at: Optional[str] = None

    # Tags and notes
    tags: List[str] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "model_id": self.model_id,
            "model_type": self.model_type,
            "version": self.version,
            "trained_at": self.trained_at,
            "training_start_date": self.training_start_date,
            "training_end_date": self.training_end_date,
            "training_samples": self.training_samples,
            "feature_names": self.feature_names,
            "feature_count": self.feature_count,
            "train_ic": self.train_ic,
            "val_ic": self.val_ic,
            "oos_ic": self.oos_ic,
            "train_r2": self.train_r2,
            "val_r2": self.val_r2,
            "oos_sharpe": self.oos_sharpe,
            "hyperparameters": self.hyperparameters,
            "is_production": self.is_production,
            "deployed_at": self.deployed_at,
            "tags": self.tags,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ModelMetadata:
        """Create from dictionary."""
        return cls(
            model_id=data["model_id"],
            model_type=data["model_type"],
            version=data["version"],
            trained_at=data["trained_at"],
            training_start_date=data["training_start_date"],
            training_end_date=data["training_end_date"],
            training_samples=data["training_samples"],
            feature_names=data["feature_names"],
            feature_count=data["feature_count"],
            train_ic=data["train_ic"],
            val_ic=data["val_ic"],
            oos_ic=data.get("oos_ic"),
            train_r2=data.get("train_r2", 0.0),
            val_r2=data.get("val_r2", 0.0),
            oos_sharpe=data.get("oos_sharpe"),
            hyperparameters=data.get("hyperparameters", {}),
            is_production=data.get("is_production", False),
            deployed_at=data.get("deployed_at"),
            tags=data.get("tags", []),
            notes=data.get("notes", ""),
        )


class ModelRegistry:
    """
    Registry for managing ML model versions.

    Storage structure:
        models/registry/
            index.json          # Model index with metadata
            production.json     # Current production model info
            xgboost_v1/
                model.pkl       # Serialized model
                metadata.json   # Model metadata
            xgboost_v2/
                model.pkl
                metadata.json
            ...
    """

    def __init__(self, registry_path: Optional[Path] = None):
        """
        Initialize model registry.

        Args:
            registry_path: Path to registry directory. Defaults to models/registry/
        """
        self.registry_path = Path(registry_path) if registry_path else DEFAULT_REGISTRY_PATH
        self.registry_path.mkdir(parents=True, exist_ok=True)

        self.index_path = self.registry_path / "index.json"
        self.production_path = self.registry_path / "production.json"

        # Load or initialize index
        self._index: Dict[str, ModelMetadata] = {}
        self._load_index()

        LOGGER.info(f"Model registry initialized at {self.registry_path}")
        LOGGER.info(f"Registered models: {len(self._index)}")

    def _load_index(self) -> None:
        """Load model index from disk."""
        if self.index_path.exists():
            with open(self.index_path) as f:
                data = json.load(f)
                self._index = {
                    model_id: ModelMetadata.from_dict(meta)
                    for model_id, meta in data.items()
                }

    def _save_index(self) -> None:
        """Save model index to disk."""
        data = {
            model_id: meta.to_dict()
            for model_id, meta in self._index.items()
        }
        with open(self.index_path, "w") as f:
            json.dump(data, f, indent=2)

    def register(
        self,
        model: Any,
        model_type: str,
        training_start_date: str,
        training_end_date: str,
        training_samples: int,
        feature_names: List[str],
        train_ic: float,
        val_ic: float,
        train_r2: float = 0.0,
        val_r2: float = 0.0,
        hyperparameters: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        notes: str = "",
    ) -> ModelMetadata:
        """
        Register a new model in the registry.

        Args:
            model: Trained model object
            model_type: Type of model (xgboost, ensemble, etc.)
            training_start_date: Training data start date
            training_end_date: Training data end date
            training_samples: Number of training samples
            feature_names: List of feature names
            train_ic: Training information coefficient
            val_ic: Validation information coefficient
            train_r2: Training R-squared
            val_r2: Validation R-squared
            hyperparameters: Model hyperparameters
            tags: Optional tags for filtering
            notes: Optional notes

        Returns:
            ModelMetadata for the registered model
        """
        # Generate version number
        version = self._get_next_version(model_type)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_id = f"{model_type}_v{version.replace('.', '_')}_{timestamp}"

        LOGGER.info(f"Registering model: {model_id}")

        # Create model directory
        model_dir = self.registry_path / model_id
        model_dir.mkdir(parents=True, exist_ok=True)

        # Save model
        model_path = model_dir / "model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        # Create metadata
        metadata = ModelMetadata(
            model_id=model_id,
            model_type=model_type,
            version=version,
            trained_at=datetime.now().isoformat(),
            training_start_date=training_start_date,
            training_end_date=training_end_date,
            training_samples=training_samples,
            feature_names=feature_names,
            feature_count=len(feature_names),
            train_ic=train_ic,
            val_ic=val_ic,
            train_r2=train_r2,
            val_r2=val_r2,
            hyperparameters=hyperparameters or {},
            tags=tags or [],
            notes=notes,
        )

        # Save metadata
        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

        # Update index
        self._index[model_id] = metadata
        self._save_index()

        LOGGER.info(f"Model registered: {model_id} (v{version})")
        LOGGER.info(f"  Train IC: {train_ic:.4f}, Val IC: {val_ic:.4f}")

        return metadata

    def _get_next_version(self, model_type: str) -> str:
        """Get next version number for model type."""
        # Find existing versions of this model type
        existing_versions = []
        for model_id, meta in self._index.items():
            if meta.model_type == model_type:
                try:
                    parts = meta.version.split(".")
                    if len(parts) >= 1:
                        existing_versions.append(int(parts[0]))
                except (ValueError, IndexError):
                    pass

        # Increment major version
        if existing_versions:
            next_major = max(existing_versions) + 1
        else:
            next_major = 1

        return f"{next_major}.0.0"

    def load(self, model_id: str) -> Tuple[Any, ModelMetadata]:
        """
        Load a model by ID.

        Args:
            model_id: Model identifier

        Returns:
            (model, metadata) tuple
        """
        if model_id not in self._index:
            raise KeyError(f"Model not found: {model_id}")

        model_dir = self.registry_path / model_id
        model_path = model_dir / "model.pkl"

        if not model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        metadata = self._index[model_id]

        LOGGER.info(f"Loaded model: {model_id}")

        return model, metadata

    def load_latest(self, model_type: Optional[str] = None) -> Tuple[Any, ModelMetadata]:
        """
        Load the latest model.

        Args:
            model_type: Filter by model type (optional)

        Returns:
            (model, metadata) tuple
        """
        # Filter by model type if specified
        candidates = self._index.values()
        if model_type:
            candidates = [m for m in candidates if m.model_type == model_type]

        if not candidates:
            raise ValueError(f"No models found for type: {model_type}")

        # Sort by trained_at timestamp
        latest = max(candidates, key=lambda m: m.trained_at)

        return self.load(latest.model_id)

    def load_production(self) -> Tuple[Any, ModelMetadata]:
        """
        Load the current production model.

        Returns:
            (model, metadata) tuple
        """
        if not self.production_path.exists():
            raise ValueError("No production model deployed")

        with open(self.production_path) as f:
            prod_info = json.load(f)

        return self.load(prod_info["model_id"])

    def deploy_to_production(self, model_id: str) -> ModelMetadata:
        """
        Deploy a model to production.

        Args:
            model_id: Model identifier to deploy

        Returns:
            Updated ModelMetadata
        """
        if model_id not in self._index:
            raise KeyError(f"Model not found: {model_id}")

        # Unset previous production model
        for meta in self._index.values():
            if meta.is_production:
                meta.is_production = False
                meta.deployed_at = None

        # Set new production model
        metadata = self._index[model_id]
        metadata.is_production = True
        metadata.deployed_at = datetime.now().isoformat()

        # Save production pointer
        with open(self.production_path, "w") as f:
            json.dump({
                "model_id": model_id,
                "deployed_at": metadata.deployed_at,
            }, f, indent=2)

        # Update index
        self._save_index()

        LOGGER.info(f"Deployed model to production: {model_id}")

        return metadata

    def get_production_model_id(self) -> Optional[str]:
        """Get the current production model ID."""
        if self.production_path.exists():
            with open(self.production_path) as f:
                prod_info = json.load(f)
                return prod_info["model_id"]
        return None

    def list_models(
        self,
        model_type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
    ) -> List[ModelMetadata]:
        """
        List registered models.

        Args:
            model_type: Filter by model type
            tags: Filter by tags (any match)
            limit: Maximum number of models to return

        Returns:
            List of ModelMetadata
        """
        models = list(self._index.values())

        # Filter by model type
        if model_type:
            models = [m for m in models if m.model_type == model_type]

        # Filter by tags
        if tags:
            models = [m for m in models if any(t in m.tags for t in tags)]

        # Sort by trained_at (newest first)
        models.sort(key=lambda m: m.trained_at, reverse=True)

        return models[:limit]

    def compare_models(
        self,
        model_ids: Optional[List[str]] = None,
        model_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compare multiple models.

        Args:
            model_ids: List of model IDs to compare
            model_type: Compare all models of this type

        Returns:
            DataFrame with comparison metrics
        """
        if model_ids:
            models = [self._index[mid] for mid in model_ids if mid in self._index]
        elif model_type:
            models = [m for m in self._index.values() if m.model_type == model_type]
        else:
            models = list(self._index.values())

        if not models:
            return pd.DataFrame()

        rows = []
        for meta in models:
            rows.append({
                "model_id": meta.model_id,
                "model_type": meta.model_type,
                "version": meta.version,
                "trained_at": meta.trained_at,
                "train_ic": meta.train_ic,
                "val_ic": meta.val_ic,
                "oos_ic": meta.oos_ic,
                "oos_sharpe": meta.oos_sharpe,
                "train_r2": meta.train_r2,
                "val_r2": meta.val_r2,
                "feature_count": meta.feature_count,
                "training_samples": meta.training_samples,
                "is_production": meta.is_production,
            })

        df = pd.DataFrame(rows)
        df = df.sort_values("val_ic", ascending=False)

        return df

    def get_best_model(
        self,
        model_type: Optional[str] = None,
        metric: str = "val_ic",
    ) -> ModelMetadata:
        """
        Get the best performing model.

        Args:
            model_type: Filter by model type
            metric: Metric to optimize (val_ic, oos_ic, oos_sharpe)

        Returns:
            Best ModelMetadata
        """
        models = self.list_models(model_type=model_type)

        if not models:
            raise ValueError(f"No models found for type: {model_type}")

        # Sort by metric (descending)
        def get_metric(m: ModelMetadata) -> float:
            val = getattr(m, metric, None)
            return val if val is not None else float("-inf")

        best = max(models, key=get_metric)

        return best

    def update_oos_metrics(
        self,
        model_id: str,
        oos_ic: Optional[float] = None,
        oos_sharpe: Optional[float] = None,
    ) -> ModelMetadata:
        """
        Update out-of-sample metrics for a model.

        Args:
            model_id: Model identifier
            oos_ic: Out-of-sample information coefficient
            oos_sharpe: Out-of-sample Sharpe ratio

        Returns:
            Updated ModelMetadata
        """
        if model_id not in self._index:
            raise KeyError(f"Model not found: {model_id}")

        metadata = self._index[model_id]

        if oos_ic is not None:
            metadata.oos_ic = oos_ic
        if oos_sharpe is not None:
            metadata.oos_sharpe = oos_sharpe

        # Update metadata file
        model_dir = self.registry_path / model_id
        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

        self._save_index()

        LOGGER.info(f"Updated OOS metrics for {model_id}: IC={oos_ic}, Sharpe={oos_sharpe}")

        return metadata

    def delete_model(self, model_id: str, archive: bool = True) -> None:
        """
        Delete a model from the registry.

        Args:
            model_id: Model identifier
            archive: If True, move to archive instead of permanent delete
        """
        if model_id not in self._index:
            raise KeyError(f"Model not found: {model_id}")

        model_dir = self.registry_path / model_id

        if archive:
            archive_dir = self.registry_path / "archive"
            archive_dir.mkdir(exist_ok=True)
            shutil.move(str(model_dir), str(archive_dir / model_id))
            LOGGER.info(f"Archived model: {model_id}")
        else:
            shutil.rmtree(model_dir)
            LOGGER.info(f"Deleted model: {model_id}")

        del self._index[model_id]
        self._save_index()

    def cleanup_old_models(
        self,
        keep_last_n: int = 5,
        model_type: Optional[str] = None,
        archive: bool = True,
    ) -> List[str]:
        """
        Clean up old model versions, keeping the N most recent.

        Args:
            keep_last_n: Number of recent models to keep
            model_type: Only clean up models of this type
            archive: If True, archive instead of delete

        Returns:
            List of deleted model IDs
        """
        models = self.list_models(model_type=model_type)

        # Keep production model
        production_id = self.get_production_model_id()
        models_to_keep = set()
        if production_id:
            models_to_keep.add(production_id)

        # Keep last N by trained_at
        models_sorted = sorted(models, key=lambda m: m.trained_at, reverse=True)
        for m in models_sorted[:keep_last_n]:
            models_to_keep.add(m.model_id)

        # Delete the rest
        deleted = []
        for m in models:
            if m.model_id not in models_to_keep:
                self.delete_model(m.model_id, archive=archive)
                deleted.append(m.model_id)

        LOGGER.info(f"Cleaned up {len(deleted)} old models (kept {len(models_to_keep)})")

        return deleted

    def get_model_stats(self) -> Dict[str, Any]:
        """Get summary statistics about registered models."""
        if not self._index:
            return {
                "total_models": 0,
                "model_types": {},
                "production_model": None,
            }

        model_types = {}
        for meta in self._index.values():
            if meta.model_type not in model_types:
                model_types[meta.model_type] = 0
            model_types[meta.model_type] += 1

        production_id = self.get_production_model_id()

        return {
            "total_models": len(self._index),
            "model_types": model_types,
            "production_model": production_id,
            "latest_model": max(self._index.values(), key=lambda m: m.trained_at).model_id,
            "best_val_ic": max(self._index.values(), key=lambda m: m.val_ic).model_id,
        }


def create_registry(path: Optional[Path] = None) -> ModelRegistry:
    """
    Create a new model registry.

    Args:
        path: Path to registry directory

    Returns:
        ModelRegistry instance
    """
    return ModelRegistry(path)
