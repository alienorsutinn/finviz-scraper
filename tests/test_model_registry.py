"""Tests for model registry."""
from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xgboost as xgb

from finviz_weekly.model_registry import (
    ModelMetadata,
    ModelRegistry,
    create_registry,
)


@pytest.fixture
def mock_model():
    """Create a simple mock model for testing."""
    X = pd.DataFrame(np.random.randn(100, 5), columns=[f"feature_{i}" for i in range(5)])
    y = pd.Series(np.random.randn(100))

    model = xgb.XGBRegressor(n_estimators=10, max_depth=3, random_state=42)
    model.fit(X, y)

    return model


@pytest.fixture
def temp_registry_path():
    """Create a temporary directory for registry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "registry"


class TestModelMetadata:
    """Tests for ModelMetadata dataclass."""

    def test_create_metadata(self):
        """Test creating metadata."""
        metadata = ModelMetadata(
            model_id="xgboost_v1_20240115_120000",
            model_type="xgboost",
            version="1.0.0",
            trained_at="2024-01-15T12:00:00",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=1000,
            feature_names=["f1", "f2", "f3"],
            feature_count=3,
            train_ic=0.06,
            val_ic=0.05,
        )

        assert metadata.model_id == "xgboost_v1_20240115_120000"
        assert metadata.model_type == "xgboost"
        assert metadata.version == "1.0.0"
        assert metadata.feature_count == 3

    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary."""
        metadata = ModelMetadata(
            model_id="test_model",
            model_type="xgboost",
            version="1.0.0",
            trained_at="2024-01-15T12:00:00",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=["f1", "f2"],
            feature_count=2,
            train_ic=0.05,
            val_ic=0.04,
        )

        data = metadata.to_dict()

        assert isinstance(data, dict)
        assert data["model_id"] == "test_model"
        assert data["model_type"] == "xgboost"
        assert data["feature_names"] == ["f1", "f2"]

    def test_metadata_from_dict(self):
        """Test creating metadata from dictionary."""
        data = {
            "model_id": "from_dict_model",
            "model_type": "ensemble",
            "version": "2.0.0",
            "trained_at": "2024-01-15T12:00:00",
            "training_start_date": "2023-01-01",
            "training_end_date": "2024-01-01",
            "training_samples": 1000,
            "feature_names": ["a", "b", "c"],
            "feature_count": 3,
            "train_ic": 0.07,
            "val_ic": 0.06,
            "oos_ic": 0.05,
            "oos_sharpe": 1.2,
        }

        metadata = ModelMetadata.from_dict(data)

        assert metadata.model_id == "from_dict_model"
        assert metadata.model_type == "ensemble"
        assert metadata.version == "2.0.0"
        assert metadata.oos_ic == 0.05
        assert metadata.oos_sharpe == 1.2

    def test_metadata_defaults(self):
        """Test metadata default values."""
        metadata = ModelMetadata(
            model_id="test",
            model_type="xgboost",
            version="1.0.0",
            trained_at="2024-01-15T12:00:00",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=[],
            feature_count=0,
            train_ic=0.0,
            val_ic=0.0,
        )

        assert metadata.oos_ic is None
        assert metadata.oos_sharpe is None
        assert metadata.is_production is False
        assert metadata.deployed_at is None
        assert metadata.tags == []
        assert metadata.notes == ""


class TestModelRegistry:
    """Tests for ModelRegistry."""

    def test_init(self, temp_registry_path):
        """Test registry initialization."""
        registry = ModelRegistry(temp_registry_path)

        assert registry.registry_path == temp_registry_path
        assert temp_registry_path.exists()
        assert registry.index_path.exists() is False  # No models yet

    def test_init_with_existing_models(self, temp_registry_path, mock_model):
        """Test loading existing registry."""
        # Create a registry and add a model
        registry1 = ModelRegistry(temp_registry_path)
        registry1.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1", "f2"],
            train_ic=0.05,
            val_ic=0.04,
        )

        # Create new registry instance - should load existing models
        registry2 = ModelRegistry(temp_registry_path)

        assert len(registry2.list_models()) == 1

    def test_register_model(self, temp_registry_path, mock_model):
        """Test registering a model."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=["feature_0", "feature_1", "feature_2"],
            train_ic=0.06,
            val_ic=0.05,
            train_r2=0.8,
            val_r2=0.7,
            hyperparameters={"max_depth": 3},
            tags=["test", "xgboost"],
            notes="Test model",
        )

        assert metadata.model_type == "xgboost"
        assert metadata.version == "1.0.0"
        assert metadata.train_ic == 0.06
        assert metadata.val_ic == 0.05
        assert "test" in metadata.tags

        # Check model file exists
        model_dir = temp_registry_path / metadata.model_id
        assert model_dir.exists()
        assert (model_dir / "model.pkl").exists()
        assert (model_dir / "metadata.json").exists()

    def test_register_multiple_models(self, temp_registry_path, mock_model):
        """Test registering multiple models."""
        registry = ModelRegistry(temp_registry_path)

        # Register first model
        meta1 = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=["f1", "f2"],
            train_ic=0.05,
            val_ic=0.04,
        )

        # Register second model
        meta2 = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-06-01",
            training_end_date="2024-06-01",
            training_samples=600,
            feature_names=["f1", "f2"],
            train_ic=0.06,
            val_ic=0.05,
        )

        assert meta1.version == "1.0.0"
        assert meta2.version == "2.0.0"
        assert len(registry.list_models()) == 2

    def test_load_model(self, temp_registry_path, mock_model):
        """Test loading a model."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1", "f2"],
            train_ic=0.05,
            val_ic=0.04,
        )

        loaded_model, loaded_metadata = registry.load(metadata.model_id)

        assert loaded_model is not None
        assert loaded_metadata.model_id == metadata.model_id
        assert loaded_metadata.train_ic == 0.05

    def test_load_nonexistent_model(self, temp_registry_path):
        """Test loading a model that doesn't exist."""
        registry = ModelRegistry(temp_registry_path)

        with pytest.raises(KeyError, match="Model not found"):
            registry.load("nonexistent_model")

    def test_load_latest(self, temp_registry_path, mock_model):
        """Test loading the latest model."""
        registry = ModelRegistry(temp_registry_path)

        # Register two models
        registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.04,
            val_ic=0.03,
        )

        meta2 = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-06-01",
            training_end_date="2024-06-01",
            training_samples=200,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        loaded_model, loaded_metadata = registry.load_latest(model_type="xgboost")

        assert loaded_metadata.model_id == meta2.model_id

    def test_deploy_to_production(self, temp_registry_path, mock_model):
        """Test deploying a model to production."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        deployed_metadata = registry.deploy_to_production(metadata.model_id)

        assert deployed_metadata.is_production is True
        assert deployed_metadata.deployed_at is not None
        assert registry.get_production_model_id() == metadata.model_id

    def test_load_production_model(self, temp_registry_path, mock_model):
        """Test loading the production model."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        registry.deploy_to_production(metadata.model_id)

        loaded_model, loaded_metadata = registry.load_production()

        assert loaded_metadata.model_id == metadata.model_id
        assert loaded_metadata.is_production is True

    def test_load_production_no_deployment(self, temp_registry_path):
        """Test loading production model when none deployed."""
        registry = ModelRegistry(temp_registry_path)

        with pytest.raises(ValueError, match="No production model deployed"):
            registry.load_production()

    def test_list_models(self, temp_registry_path, mock_model):
        """Test listing models."""
        registry = ModelRegistry(temp_registry_path)

        # Register multiple models
        for i in range(5):
            registry.register(
                model=mock_model,
                model_type="xgboost" if i < 3 else "ensemble",
                training_start_date="2023-01-01",
                training_end_date="2024-01-01",
                training_samples=100,
                feature_names=["f1"],
                train_ic=0.05 + i * 0.01,
                val_ic=0.04 + i * 0.01,
            )

        # List all
        all_models = registry.list_models()
        assert len(all_models) == 5

        # Filter by type
        xgb_models = registry.list_models(model_type="xgboost")
        assert len(xgb_models) == 3

        ensemble_models = registry.list_models(model_type="ensemble")
        assert len(ensemble_models) == 2

    def test_list_models_with_limit(self, temp_registry_path, mock_model):
        """Test listing models with limit."""
        registry = ModelRegistry(temp_registry_path)

        for i in range(10):
            registry.register(
                model=mock_model,
                model_type="xgboost",
                training_start_date="2023-01-01",
                training_end_date="2024-01-01",
                training_samples=100,
                feature_names=["f1"],
                train_ic=0.05,
                val_ic=0.04,
            )

        models = registry.list_models(limit=5)
        assert len(models) == 5

    def test_compare_models(self, temp_registry_path, mock_model):
        """Test comparing models."""
        registry = ModelRegistry(temp_registry_path)

        for i in range(3):
            registry.register(
                model=mock_model,
                model_type="xgboost",
                training_start_date="2023-01-01",
                training_end_date="2024-01-01",
                training_samples=100 + i * 50,
                feature_names=["f1"],
                train_ic=0.05 + i * 0.01,
                val_ic=0.04 + i * 0.01,
            )

        df = registry.compare_models()

        assert len(df) == 3
        assert "model_id" in df.columns
        assert "val_ic" in df.columns
        assert "train_ic" in df.columns

    def test_get_best_model(self, temp_registry_path, mock_model):
        """Test getting the best model."""
        registry = ModelRegistry(temp_registry_path)

        # Register models with different IC values
        for ic in [0.03, 0.05, 0.04]:
            registry.register(
                model=mock_model,
                model_type="xgboost",
                training_start_date="2023-01-01",
                training_end_date="2024-01-01",
                training_samples=100,
                feature_names=["f1"],
                train_ic=ic,
                val_ic=ic,
            )

        best = registry.get_best_model(metric="val_ic")

        assert best.val_ic == 0.05

    def test_update_oos_metrics(self, temp_registry_path, mock_model):
        """Test updating OOS metrics."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        assert metadata.oos_ic is None
        assert metadata.oos_sharpe is None

        updated = registry.update_oos_metrics(
            model_id=metadata.model_id,
            oos_ic=0.035,
            oos_sharpe=1.5,
        )

        assert updated.oos_ic == 0.035
        assert updated.oos_sharpe == 1.5

        # Verify persisted
        _, loaded = registry.load(metadata.model_id)
        assert loaded.oos_ic == 0.035
        assert loaded.oos_sharpe == 1.5

    def test_delete_model(self, temp_registry_path, mock_model):
        """Test deleting a model."""
        registry = ModelRegistry(temp_registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        assert len(registry.list_models()) == 1

        registry.delete_model(metadata.model_id, archive=True)

        assert len(registry.list_models()) == 0

        # Check archived
        archive_dir = temp_registry_path / "archive"
        assert archive_dir.exists()
        assert (archive_dir / metadata.model_id).exists()

    def test_cleanup_old_models(self, temp_registry_path, mock_model):
        """Test cleaning up old models."""
        registry = ModelRegistry(temp_registry_path)

        # Register 10 models
        for i in range(10):
            registry.register(
                model=mock_model,
                model_type="xgboost",
                training_start_date="2023-01-01",
                training_end_date="2024-01-01",
                training_samples=100,
                feature_names=["f1"],
                train_ic=0.05,
                val_ic=0.04,
            )

        assert len(registry.list_models()) == 10

        deleted = registry.cleanup_old_models(keep_last_n=3)

        assert len(deleted) == 7
        assert len(registry.list_models()) == 3

    def test_get_model_stats(self, temp_registry_path, mock_model):
        """Test getting model statistics."""
        registry = ModelRegistry(temp_registry_path)

        # Empty registry
        stats = registry.get_model_stats()
        assert stats["total_models"] == 0
        assert stats["production_model"] is None

        # Add models
        meta1 = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )

        registry.register(
            model=mock_model,
            model_type="ensemble",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=100,
            feature_names=["f1"],
            train_ic=0.06,
            val_ic=0.05,
        )

        registry.deploy_to_production(meta1.model_id)

        stats = registry.get_model_stats()

        assert stats["total_models"] == 2
        assert stats["production_model"] == meta1.model_id
        assert stats["model_types"]["xgboost"] == 1
        assert stats["model_types"]["ensemble"] == 1


class TestCreateRegistry:
    """Tests for create_registry helper function."""

    def test_create_registry(self, temp_registry_path):
        """Test creating a registry."""
        registry = create_registry(temp_registry_path)

        assert isinstance(registry, ModelRegistry)
        assert registry.registry_path == temp_registry_path

    def test_create_registry_default_path(self):
        """Test creating a registry with default path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            import os
            old_cwd = os.getcwd()
            os.chdir(tmpdir)

            try:
                registry = create_registry()
                assert isinstance(registry, ModelRegistry)
            finally:
                os.chdir(old_cwd)
