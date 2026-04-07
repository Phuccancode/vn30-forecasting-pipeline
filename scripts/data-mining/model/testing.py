import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import joblib
import matplotlib.pyplot as plt
from transformer import TimeSeriesTransformer
from load_data import load_or_preprocess

# ===== 1. Load data =====
data = load_or_preprocess()

X_test = data["X_test"]
y_test = data["y_test"]

# ===== 2. DataLoader =====
BATCH_SIZE = 32
test_loader = DataLoader(TensorDataset(X_test, y_test), batch_size=BATCH_SIZE, shuffle=False)

# ===== 3. Model =====
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = TimeSeriesTransformer(input_dim=9).to(device)
model.load_state_dict(torch.load("scripts/data-mining/storage/stock_transformer.pth"))

criterion = nn.MSELoss()

# ===== 4. Validation =====
EPOCHS = 10

model.eval()
val_loss = 0
all_preds = []

with torch.no_grad():
    for X_batch, y_batch in test_loader:
        X_batch = X_batch.to(device)
        y_batch = y_batch.to(device)

        output = model(X_batch)
        loss = criterion(output.squeeze(), y_batch)
        val_loss += loss.item()

        all_preds.append(output.cpu()) 

val_loss /= len(test_loader)
print(f"Validation Loss: {val_loss:.4f}")

pred_np = torch.cat(all_preds, dim=0).numpy()
y_np = y_test.cpu().numpy()

scaler = joblib.load("scripts/data-mining/storage/scaler_y.pkl")
pred_real = scaler.inverse_transform(pred_np)
y_real = scaler.inverse_transform(y_np)

plt.plot(y_real[:200], label="True")
plt.plot(pred_real[:200], label="Pred")
plt.legend()
plt.show()

plt.savefig("scripts/data-mining/storage/plot.png")
print("Saved the comparison graph to plot.png")