import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from transformer import TimeSeriesTransformer
from load_data import load_or_preprocess

# ===== 1. Load data =====
data = load_or_preprocess()

X_train = data["X_train"]
y_train = data["y_train"]

# ===== 2. DataLoader =====
BATCH_SIZE = 512
train_loader = DataLoader(
    TensorDataset(X_train, y_train),
    batch_size=BATCH_SIZE,
    shuffle=True,
    num_workers=2,
    pin_memory=True
)

# ===== 3. Model =====
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = TimeSeriesTransformer(input_dim=9).to(device)

criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=2e-3)

# ===== 4. Training =====
EPOCHS = 10

for epoch in range(EPOCHS):
    model.train()
    train_loss = 0

    for X_batch, y_batch in train_loader:
        X_batch = X_batch.to(device)
        y_batch = y_batch.to(device)

        optimizer.zero_grad()

        output = model(X_batch)
        loss = criterion(output.squeeze(), y_batch)

        loss.backward()
        optimizer.step()

        train_loss += loss.item()

    train_loss /= len(train_loader)

    print(f"Epoch {epoch+1}/{EPOCHS} | Train Loss: {train_loss:.4f}")

# ===== 5. Save model =====
torch.save(model.state_dict(), "scripts/data-mining/storage/stock_transformer.pth")