"""
Transformer-based Time Series Models

Advanced ML models for:
- Stock price prediction
- Factor return forecasting
- Volatility prediction
- Regime classification
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

# Optional deep learning imports
try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.utils.data import Dataset, DataLoader
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    # Dummy classes
    class nn:
        class Module:
            pass


class ModelType(Enum):
    """Types of transformer models."""
    PRICE_PREDICTOR = "price_predictor"
    FACTOR_FORECASTER = "factor_forecaster"
    VOLATILITY_PREDICTOR = "volatility_predictor"
    REGIME_CLASSIFIER = "regime_classifier"


@dataclass
class TransformerConfig:
    """Configuration for transformer models."""
    # Architecture
    d_model: int = 64  # Model dimension
    n_heads: int = 4  # Number of attention heads
    n_encoder_layers: int = 2
    n_decoder_layers: int = 2
    d_feedforward: int = 256
    dropout: float = 0.1

    # Input/Output
    seq_length: int = 60  # Input sequence length (days)
    pred_length: int = 5  # Prediction horizon (days)
    n_features: int = 10  # Number of input features

    # Training
    batch_size: int = 32
    learning_rate: float = 0.001
    n_epochs: int = 100
    early_stopping_patience: int = 10

    # Regularization
    weight_decay: float = 0.01
    label_smoothing: float = 0.0


@dataclass
class TrainingResult:
    """Results from model training."""
    model_type: ModelType
    train_loss: List[float]
    val_loss: List[float]
    best_epoch: int
    best_val_loss: float
    training_time_seconds: float
    metrics: Dict[str, float] = field(default_factory=dict)


# =============================================================================
# Positional Encoding
# =============================================================================

if TORCH_AVAILABLE:
    class PositionalEncoding(nn.Module):
        """Positional encoding for transformer."""

        def __init__(self, d_model: int, max_len: int = 5000, dropout: float = 0.1):
            super().__init__()
            self.dropout = nn.Dropout(p=dropout)

            # Create positional encoding matrix
            pe = torch.zeros(max_len, d_model)
            position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
            div_term = torch.exp(
                torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model)
            )

            pe[:, 0::2] = torch.sin(position * div_term)
            pe[:, 1::2] = torch.cos(position * div_term)
            pe = pe.unsqueeze(0).transpose(0, 1)

            self.register_buffer('pe', pe)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            x = x + self.pe[:x.size(0), :]
            return self.dropout(x)


# =============================================================================
# Transformer Models
# =============================================================================

if TORCH_AVAILABLE:
    class TimeSeriesTransformer(nn.Module):
        """
        Transformer model for time series prediction.

        Based on the architecture from "Attention Is All You Need"
        adapted for financial time series.
        """

        def __init__(self, config: TransformerConfig):
            super().__init__()
            self.config = config

            # Input projection
            self.input_projection = nn.Linear(config.n_features, config.d_model)

            # Positional encoding
            self.pos_encoder = PositionalEncoding(
                config.d_model,
                max_len=config.seq_length + config.pred_length,
                dropout=config.dropout,
            )

            # Transformer encoder
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=config.d_model,
                nhead=config.n_heads,
                dim_feedforward=config.d_feedforward,
                dropout=config.dropout,
                batch_first=True,
            )
            self.transformer_encoder = nn.TransformerEncoder(
                encoder_layer,
                num_layers=config.n_encoder_layers,
            )

            # Output projection
            self.output_projection = nn.Linear(config.d_model, 1)

            # Initialize weights
            self._init_weights()

        def _init_weights(self):
            """Initialize model weights."""
            for p in self.parameters():
                if p.dim() > 1:
                    nn.init.xavier_uniform_(p)

        def forward(
            self,
            src: torch.Tensor,
            src_mask: Optional[torch.Tensor] = None,
        ) -> torch.Tensor:
            """
            Forward pass.

            Args:
                src: Input tensor (batch, seq_len, n_features)
                src_mask: Optional attention mask

            Returns:
                Output tensor (batch, pred_length)
            """
            # Project input to model dimension
            x = self.input_projection(src)

            # Add positional encoding
            x = self.pos_encoder(x)

            # Transformer encoder
            x = self.transformer_encoder(x, mask=src_mask)

            # Take last pred_length outputs
            x = x[:, -self.config.pred_length:, :]

            # Project to output
            x = self.output_projection(x)

            return x.squeeze(-1)


    class TemporalFusionTransformer(nn.Module):
        """
        Simplified Temporal Fusion Transformer for interpretable forecasting.

        Includes:
        - Variable selection network
        - Temporal attention
        - Quantile outputs
        """

        def __init__(self, config: TransformerConfig, n_quantiles: int = 3):
            super().__init__()
            self.config = config
            self.n_quantiles = n_quantiles

            # Variable selection
            self.variable_selection = nn.Sequential(
                nn.Linear(config.n_features, config.d_model),
                nn.ReLU(),
                nn.Linear(config.d_model, config.n_features),
                nn.Softmax(dim=-1),
            )

            # Input processing
            self.input_projection = nn.Linear(config.n_features, config.d_model)

            # LSTM encoder
            self.lstm = nn.LSTM(
                input_size=config.d_model,
                hidden_size=config.d_model,
                num_layers=2,
                batch_first=True,
                dropout=config.dropout,
            )

            # Attention
            self.attention = nn.MultiheadAttention(
                embed_dim=config.d_model,
                num_heads=config.n_heads,
                dropout=config.dropout,
                batch_first=True,
            )

            # Output layers
            self.output_projection = nn.Linear(config.d_model, n_quantiles)

        def forward(
            self,
            x: torch.Tensor,
        ) -> Tuple[torch.Tensor, torch.Tensor]:
            """
            Forward pass.

            Args:
                x: Input tensor (batch, seq_len, n_features)

            Returns:
                Tuple of (predictions, attention_weights)
            """
            batch_size, seq_len, n_features = x.shape

            # Variable selection
            var_weights = self.variable_selection(x.mean(dim=1))  # (batch, n_features)
            x = x * var_weights.unsqueeze(1)

            # Project input
            x = self.input_projection(x)

            # LSTM encoding
            lstm_out, _ = self.lstm(x)

            # Self-attention
            attn_out, attn_weights = self.attention(lstm_out, lstm_out, lstm_out)

            # Output (take last position)
            out = self.output_projection(attn_out[:, -1, :])

            return out, attn_weights


    class RegimeClassifier(nn.Module):
        """
        Transformer-based market regime classifier.

        Classifies market conditions into:
        - Bull trend
        - Bear trend
        - High volatility
        - Low volatility
        - Mean-reverting
        """

        def __init__(self, config: TransformerConfig, n_regimes: int = 4):
            super().__init__()
            self.config = config
            self.n_regimes = n_regimes

            # Input projection
            self.input_projection = nn.Linear(config.n_features, config.d_model)

            # Positional encoding
            self.pos_encoder = PositionalEncoding(config.d_model)

            # Transformer encoder
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=config.d_model,
                nhead=config.n_heads,
                dim_feedforward=config.d_feedforward,
                dropout=config.dropout,
                batch_first=True,
            )
            self.transformer = nn.TransformerEncoder(
                encoder_layer,
                num_layers=config.n_encoder_layers,
            )

            # Classification head
            self.classifier = nn.Sequential(
                nn.Linear(config.d_model, config.d_model // 2),
                nn.ReLU(),
                nn.Dropout(config.dropout),
                nn.Linear(config.d_model // 2, n_regimes),
            )

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            """
            Forward pass.

            Args:
                x: Input tensor (batch, seq_len, n_features)

            Returns:
                Regime logits (batch, n_regimes)
            """
            x = self.input_projection(x)
            x = self.pos_encoder(x)
            x = self.transformer(x)

            # Global average pooling
            x = x.mean(dim=1)

            # Classification
            return self.classifier(x)


# =============================================================================
# Dataset Classes
# =============================================================================

if TORCH_AVAILABLE:
    class TimeSeriesDataset(Dataset):
        """Dataset for time series data."""

        def __init__(
            self,
            data: np.ndarray,
            targets: np.ndarray,
            seq_length: int,
            pred_length: int,
        ):
            self.data = torch.FloatTensor(data)
            self.targets = torch.FloatTensor(targets)
            self.seq_length = seq_length
            self.pred_length = pred_length

        def __len__(self) -> int:
            return len(self.data) - self.seq_length - self.pred_length + 1

        def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:
            x = self.data[idx:idx + self.seq_length]
            y = self.targets[idx + self.seq_length:idx + self.seq_length + self.pred_length]
            return x, y


# =============================================================================
# Training Functions
# =============================================================================

class TransformerTrainer:
    """Trainer for transformer models."""

    def __init__(
        self,
        config: TransformerConfig,
        model_type: ModelType = ModelType.PRICE_PREDICTOR,
    ):
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch not installed. Install with: pip install torch")

        self.config = config
        self.model_type = model_type
        self.model = self._create_model()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)

    def _create_model(self) -> nn.Module:
        """Create model based on type."""
        if self.model_type == ModelType.PRICE_PREDICTOR:
            return TimeSeriesTransformer(self.config)
        elif self.model_type == ModelType.FACTOR_FORECASTER:
            return TimeSeriesTransformer(self.config)
        elif self.model_type == ModelType.VOLATILITY_PREDICTOR:
            return TemporalFusionTransformer(self.config)
        elif self.model_type == ModelType.REGIME_CLASSIFIER:
            return RegimeClassifier(self.config)
        else:
            return TimeSeriesTransformer(self.config)

    def prepare_data(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        target_col: str,
        train_ratio: float = 0.8,
    ) -> Tuple[DataLoader, DataLoader]:
        """
        Prepare data for training.

        Args:
            df: DataFrame with features and target
            feature_cols: Feature column names
            target_col: Target column name
            train_ratio: Train/validation split ratio

        Returns:
            Tuple of (train_loader, val_loader)
        """
        # Extract arrays
        features = df[feature_cols].values
        targets = df[target_col].values

        # Normalize features
        self.feature_mean = features.mean(axis=0)
        self.feature_std = features.std(axis=0) + 1e-8
        features = (features - self.feature_mean) / self.feature_std

        # Split
        split_idx = int(len(features) * train_ratio)

        train_dataset = TimeSeriesDataset(
            features[:split_idx],
            targets[:split_idx],
            self.config.seq_length,
            self.config.pred_length,
        )

        val_dataset = TimeSeriesDataset(
            features[split_idx:],
            targets[split_idx:],
            self.config.seq_length,
            self.config.pred_length,
        )

        train_loader = DataLoader(
            train_dataset,
            batch_size=self.config.batch_size,
            shuffle=True,
        )

        val_loader = DataLoader(
            val_dataset,
            batch_size=self.config.batch_size,
            shuffle=False,
        )

        return train_loader, val_loader

    def train(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
    ) -> TrainingResult:
        """
        Train the model.

        Args:
            train_loader: Training data loader
            val_loader: Validation data loader

        Returns:
            TrainingResult
        """
        import time

        start_time = time.time()

        optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay,
        )

        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer,
            mode='min',
            factor=0.5,
            patience=5,
        )

        if self.model_type == ModelType.REGIME_CLASSIFIER:
            criterion = nn.CrossEntropyLoss()
        else:
            criterion = nn.MSELoss()

        train_losses = []
        val_losses = []
        best_val_loss = float('inf')
        best_epoch = 0
        patience_counter = 0

        for epoch in range(self.config.n_epochs):
            # Training
            self.model.train()
            train_loss = 0.0

            for batch_x, batch_y in train_loader:
                batch_x = batch_x.to(self.device)
                batch_y = batch_y.to(self.device)

                optimizer.zero_grad()

                if isinstance(self.model, TemporalFusionTransformer):
                    output, _ = self.model(batch_x)
                    loss = criterion(output[:, 1], batch_y[:, 0])  # Median quantile
                else:
                    output = self.model(batch_x)
                    loss = criterion(output, batch_y)

                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                optimizer.step()

                train_loss += loss.item()

            train_loss /= len(train_loader)
            train_losses.append(train_loss)

            # Validation
            self.model.eval()
            val_loss = 0.0

            with torch.no_grad():
                for batch_x, batch_y in val_loader:
                    batch_x = batch_x.to(self.device)
                    batch_y = batch_y.to(self.device)

                    if isinstance(self.model, TemporalFusionTransformer):
                        output, _ = self.model(batch_x)
                        loss = criterion(output[:, 1], batch_y[:, 0])
                    else:
                        output = self.model(batch_x)
                        loss = criterion(output, batch_y)

                    val_loss += loss.item()

            val_loss /= len(val_loader)
            val_losses.append(val_loss)

            scheduler.step(val_loss)

            # Early stopping
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_epoch = epoch
                patience_counter = 0
                # Save best model
                self.best_state = self.model.state_dict().copy()
            else:
                patience_counter += 1
                if patience_counter >= self.config.early_stopping_patience:
                    LOGGER.info(f"Early stopping at epoch {epoch}")
                    break

            if epoch % 10 == 0:
                LOGGER.info(
                    f"Epoch {epoch}: train_loss={train_loss:.6f}, val_loss={val_loss:.6f}"
                )

        # Restore best model
        if hasattr(self, 'best_state'):
            self.model.load_state_dict(self.best_state)

        training_time = time.time() - start_time

        return TrainingResult(
            model_type=self.model_type,
            train_loss=train_losses,
            val_loss=val_losses,
            best_epoch=best_epoch,
            best_val_loss=best_val_loss,
            training_time_seconds=training_time,
        )

    def predict(self, features: np.ndarray) -> np.ndarray:
        """
        Make predictions.

        Args:
            features: Input features (n_samples, seq_length, n_features)

        Returns:
            Predictions
        """
        self.model.eval()

        # Normalize
        features = (features - self.feature_mean) / self.feature_std

        with torch.no_grad():
            x = torch.FloatTensor(features).to(self.device)

            if isinstance(self.model, TemporalFusionTransformer):
                output, _ = self.model(x)
                return output[:, 1].cpu().numpy()  # Median
            else:
                output = self.model(x)
                return output.cpu().numpy()

    def save(self, path: str) -> None:
        """Save model to file."""
        torch.save({
            'model_state': self.model.state_dict(),
            'config': self.config,
            'feature_mean': self.feature_mean,
            'feature_std': self.feature_std,
        }, path)

    def load(self, path: str) -> None:
        """Load model from file."""
        checkpoint = torch.load(path, map_location=self.device)
        self.model.load_state_dict(checkpoint['model_state'])
        self.feature_mean = checkpoint['feature_mean']
        self.feature_std = checkpoint['feature_std']


# =============================================================================
# Convenience Functions
# =============================================================================

def create_price_predictor(
    n_features: int = 10,
    seq_length: int = 60,
    pred_length: int = 5,
) -> TransformerTrainer:
    """Create a price prediction model."""
    config = TransformerConfig(
        n_features=n_features,
        seq_length=seq_length,
        pred_length=pred_length,
    )
    return TransformerTrainer(config, ModelType.PRICE_PREDICTOR)


def create_volatility_predictor(
    n_features: int = 10,
    seq_length: int = 30,
) -> TransformerTrainer:
    """Create a volatility prediction model."""
    config = TransformerConfig(
        n_features=n_features,
        seq_length=seq_length,
        pred_length=1,
    )
    return TransformerTrainer(config, ModelType.VOLATILITY_PREDICTOR)


def create_regime_classifier(
    n_features: int = 10,
    seq_length: int = 60,
    n_regimes: int = 4,
) -> TransformerTrainer:
    """Create a regime classification model."""
    config = TransformerConfig(
        n_features=n_features,
        seq_length=seq_length,
        pred_length=1,
    )
    return TransformerTrainer(config, ModelType.REGIME_CLASSIFIER)


# =============================================================================
# Feature Engineering for Time Series
# =============================================================================

def create_time_series_features(
    df: pd.DataFrame,
    price_col: str = 'close',
    volume_col: str = 'volume',
) -> pd.DataFrame:
    """
    Create features for time series models.

    Args:
        df: DataFrame with price data
        price_col: Price column name
        volume_col: Volume column name

    Returns:
        DataFrame with features
    """
    result = df.copy()

    # Returns
    result['return_1d'] = result[price_col].pct_change()
    result['return_5d'] = result[price_col].pct_change(5)
    result['return_20d'] = result[price_col].pct_change(20)

    # Volatility
    result['volatility_5d'] = result['return_1d'].rolling(5).std()
    result['volatility_20d'] = result['return_1d'].rolling(20).std()

    # Moving averages
    result['sma_5'] = result[price_col].rolling(5).mean()
    result['sma_20'] = result[price_col].rolling(20).mean()
    result['sma_ratio'] = result['sma_5'] / result['sma_20']

    # RSI
    delta = result[price_col].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-10)
    result['rsi'] = 100 - (100 / (1 + rs))

    # Volume features
    if volume_col in result.columns:
        result['volume_sma'] = result[volume_col].rolling(20).mean()
        result['volume_ratio'] = result[volume_col] / result['volume_sma']

    return result.dropna()
