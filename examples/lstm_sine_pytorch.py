"""
Minimal LSTM example (PyTorch).

Task: next-step prediction on a noisy sine wave.

Outputs:
  - outputs/lstm_sine_prediction.png
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm


def set_seed(seed: int) -> None:
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def pick_device(requested: str) -> torch.device:
    requested = requested.lower()
    if requested == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if requested.startswith("cuda") and not torch.cuda.is_available():
        raise SystemExit("Requested CUDA device, but CUDA is not available in this PyTorch install.")
    return torch.device(requested)


def make_sine_series(n_points: int, noise_std: float, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    x = np.linspace(0.0, 24.0 * np.pi, n_points, dtype=np.float32)
    y = np.sin(x) + rng.normal(0.0, noise_std, size=n_points).astype(np.float32)
    return y


def make_sequences(series: np.ndarray, seq_len: int) -> tuple[np.ndarray, np.ndarray]:
    if seq_len < 1:
        raise ValueError("seq_len must be >= 1")
    if len(series) <= seq_len:
        raise ValueError("series must be longer than seq_len")

    x = []
    y = []
    for i in range(len(series) - seq_len):
        x.append(series[i : i + seq_len])
        y.append(series[i + seq_len])
    x_arr = np.asarray(x, dtype=np.float32)[..., None]  # (N, seq_len, 1)
    y_arr = np.asarray(y, dtype=np.float32)[..., None]  # (N, 1)
    return x_arr, y_arr


class LSTMRegressor(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_layers: int, dropout: float) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, input_size)
        out, _ = self.lstm(x)
        last = out[:, -1, :]  # (batch, hidden)
        return self.fc(last)  # (batch, 1)


@dataclass(frozen=True)
class Config:
    n_points: int
    seq_len: int
    train_frac: float
    noise_std: float
    seed: int
    batch_size: int
    epochs: int
    lr: float
    hidden_size: int
    num_layers: int
    dropout: float
    device: str
    out_dir: Path


def parse_args() -> Config:
    p = argparse.ArgumentParser()
    p.add_argument("--n-points", type=int, default=3000)
    p.add_argument("--seq-len", type=int, default=50)
    p.add_argument("--train-frac", type=float, default=0.8)
    p.add_argument("--noise-std", type=float, default=0.05)
    p.add_argument("--seed", type=int, default=123)
    p.add_argument("--batch-size", type=int, default=128)
    p.add_argument("--epochs", type=int, default=15)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--hidden-size", type=int, default=64)
    p.add_argument("--num-layers", type=int, default=2)
    p.add_argument("--dropout", type=float, default=0.1)
    p.add_argument("--device", type=str, default="auto", help="auto, cpu, cuda, cuda:0, ...")
    p.add_argument("--out-dir", type=Path, default=Path("outputs"))
    a = p.parse_args()
    return Config(
        n_points=a.n_points,
        seq_len=a.seq_len,
        train_frac=a.train_frac,
        noise_std=a.noise_std,
        seed=a.seed,
        batch_size=a.batch_size,
        epochs=a.epochs,
        lr=a.lr,
        hidden_size=a.hidden_size,
        num_layers=a.num_layers,
        dropout=a.dropout,
        device=a.device,
        out_dir=a.out_dir,
    )


def main() -> None:
    cfg = parse_args()
    set_seed(cfg.seed)
    device = pick_device(cfg.device)

    series = make_sine_series(n_points=cfg.n_points, noise_std=cfg.noise_std, seed=cfg.seed)

    # Simple standardization helps optimization.
    mu = float(series.mean())
    sigma = float(series.std() + 1e-8)
    series = (series - mu) / sigma

    x_np, y_np = make_sequences(series, seq_len=cfg.seq_len)
    n = x_np.shape[0]
    n_train = int(n * cfg.train_frac)

    x_train = torch.from_numpy(x_np[:n_train])
    y_train = torch.from_numpy(y_np[:n_train])
    x_test = torch.from_numpy(x_np[n_train:])
    y_test = torch.from_numpy(y_np[n_train:])

    train_loader = DataLoader(TensorDataset(x_train, y_train), batch_size=cfg.batch_size, shuffle=True)
    test_loader = DataLoader(TensorDataset(x_test, y_test), batch_size=cfg.batch_size, shuffle=False)

    model = LSTMRegressor(
        input_size=1,
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
        dropout=cfg.dropout,
    ).to(device)

    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)

    model.train()
    for epoch in range(1, cfg.epochs + 1):
        losses = []
        for xb, yb in tqdm(train_loader, desc=f"epoch {epoch}/{cfg.epochs}", leave=False, ascii=True):
            xb = xb.to(device)
            yb = yb.to(device)

            pred = model(xb)
            loss = criterion(pred, yb)

            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            optimizer.step()

            losses.append(float(loss.detach().cpu()))

        print(f"epoch {epoch:>2}: train_mse={np.mean(losses):.6f}")

    def eval_mse(loader: DataLoader) -> float:
        model.eval()
        losses_eval = []
        with torch.no_grad():
            for xb, yb in loader:
                xb = xb.to(device)
                yb = yb.to(device)
                pred = model(xb)
                loss = criterion(pred, yb)
                losses_eval.append(float(loss.detach().cpu()))
        model.train()
        return float(np.mean(losses_eval))

    test_mse = eval_mse(test_loader)
    print(f"test_mse={test_mse:.6f}")

    # Plot a slice of predictions.
    model.eval()
    with torch.no_grad():
        preds = model(x_test.to(device)).detach().cpu().numpy().squeeze(-1)
    truths = y_test.numpy().squeeze(-1)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = cfg.out_dir / "lstm_sine_prediction.png"

    k = min(400, len(preds))
    plt.figure(figsize=(12, 4))
    plt.plot(truths[:k], label="true")
    plt.plot(preds[:k], label="pred")
    plt.title("LSTM next-step prediction (standardized)")
    plt.xlabel("time index (test set)")
    plt.ylabel("value")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    print(f"wrote: {out_path}")


if __name__ == "__main__":
    main()
