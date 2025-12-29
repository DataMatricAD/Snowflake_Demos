import streamlit as st
import pandas as pd
import numpy as np

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from snowflake_session import get_session

st.set_page_config(page_title="5-min PyTorch on Snowflake", layout="wide")
st.title("PyTorch demo on Snowflake sample data")
st.caption("Use case: Predict late shipment risk (TPCH LINEITEM).")

@st.cache_resource
def session():
    return get_session()

@st.cache_data(ttl=3600)
def load_data(limit_rows: int = 20000) -> pd.DataFrame:
    s = session()
    q = f"""
    SELECT
      L_QUANTITY,
      L_EXTENDEDPRICE,
      L_DISCOUNT,
      L_TAX,
      L_SHIPMODE,
      IFF(L_RECEIPTDATE > L_COMMITDATE, 1, 0) AS IS_LATE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
    WHERE L_SHIPMODE IS NOT NULL
    SAMPLE (100)
    LIMIT {limit_rows}
    """
    df = s.sql(q).to_pandas()
    df["IS_LATE"] = df["IS_LATE"].astype(int)
    return df

def prep_features(df: pd.DataFrame):
    # one-hot for ship mode
    ship_dummies = pd.get_dummies(df["L_SHIPMODE"], prefix="SHIPMODE")
    X = pd.concat(
        [
            df[["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX"]].astype(float),
            ship_dummies.astype(float),
        ],
        axis=1,
    )
    y = df["IS_LATE"].astype(float)

    # normalize numeric cols only
    num_cols = ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX"]
    X[num_cols] = (X[num_cols] - X[num_cols].mean()) / (X[num_cols].std(ddof=0) + 1e-6)

    return X, y

class LogisticModel(nn.Module):
    def __init__(self, in_features: int):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(in_features, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
        )

    def forward(self, x):
        return self.net(x).squeeze(1)

def train_torch(X: pd.DataFrame, y: pd.Series, epochs=5, batch_size=256, lr=1e-2):
    X_t = torch.tensor(X.values, dtype=torch.float32)
    y_t = torch.tensor(y.values, dtype=torch.float32)

    # simple split
    n = len(X_t)
    idx = torch.randperm(n)
    split = int(0.8 * n)
    train_idx, test_idx = idx[:split], idx[split:]

    train_ds = TensorDataset(X_t[train_idx], y_t[train_idx])
    test_ds = TensorDataset(X_t[test_idx], y_t[test_idx])

    train_dl = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    test_dl = DataLoader(test_ds, batch_size=batch_size)

    model = LogisticModel(X.shape[1])
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.BCEWithLogitsLoss()

    history = []
    for ep in range(1, epochs + 1):
        model.train()
        total_loss = 0.0
        for xb, yb in train_dl:
            opt.zero_grad()
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()
            opt.step()
            total_loss += loss.item() * len(xb)

        # eval
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for xb, yb in test_dl:
                logits = model(xb)
                probs = torch.sigmoid(logits)
                preds = (probs >= 0.5).float()
                correct += (preds == yb).sum().item()
                total += len(xb)
        acc = correct / max(total, 1)

        history.append({"epoch": ep, "train_loss": total_loss / len(train_ds), "test_acc": acc})

    return model, pd.DataFrame(history)

with st.sidebar:
    st.header("Controls")
    limit_rows = st.slider("Rows to sample", 2000, 50000, 20000, step=2000)
    epochs = st.slider("Epochs", 1, 20, 5)
    run = st.button("Train model")

df = load_data(limit_rows)
st.write("### Sample data")
st.dataframe(df.head(10), use_container_width=True)

late_rate = df["IS_LATE"].mean()
c1, c2, c3 = st.columns(3)
c1.metric("Rows", f"{len(df):,}")
c2.metric("Late rate", f"{late_rate*100:.1f}%")
c3.metric("Ship modes", f"{df['L_SHIPMODE'].nunique()}")

if run:
    X, y = prep_features(df)
    model, hist = train_torch(X, y, epochs=epochs)

    st.success("Training complete.")
    st.write("### Training history")
    st.dataframe(hist, use_container_width=True)

    # quick scoring preview
    with torch.no_grad():
        X_t = torch.tensor(X.values[:20], dtype=torch.float32)
        probs = torch.sigmoid(model(X_t)).numpy()

    scored = df.head(20).copy()
    scored["LATE_PROB"] = probs
    st.write("### Example predictions (top 20)")
    st.dataframe(scored.sort_values("LATE_PROB", ascending=False), use_container_width=True)

# “Clear cache” button for local dev
st.sidebar.markdown("---")
if st.sidebar.button("Clear Streamlit cache"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()
