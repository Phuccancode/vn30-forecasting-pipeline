import os
import torch
from preprocessing import preprocess

def load_or_preprocess():
    if os.path.exists("scripts/data-mining/storage/data_cache.pt"):
        print("🔄 Load cached data...")
        return torch.load("scripts/data-mining/storage/data_cache.pt")
    else:
        print("⚙️ Preprocessing data...")
        X_train, y_train, X_test, y_test = preprocess()

        data = {
            "X_train": torch.tensor(X_train, dtype=torch.float32),
            "y_train": torch.tensor(y_train, dtype=torch.float32),
            "X_test": torch.tensor(X_test, dtype=torch.float32),
            "y_test": torch.tensor(y_test, dtype=torch.float32)
        }

        torch.save(data, "scripts/data-mining/storage/data_cache.pt")
        return data