from sklearn.preprocessing import MinMaxScaler
import joblib
from .utils import get_data, time_series_split, create_sequences_multifeature

features = [
    "open", "high", "low", "close", "volume",
    "ma10", "ma20", "ma50", "ma200"
]
target_idx = 3 # vị trí cột "close"
interval = 30

def preprocess():
    data = get_data()
    
    train_df, test_df = time_series_split(data)

    train_features = train_df[features].values
    train_target = train_df[[target_idx]].values

    test_features = test_df[features].values
    test_target = test_df[[target_idx]].values

    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()
    
    train_X_scaled = scaler_X.fit_transform(train_features)
    test_X_scaled = scaler_X.transform(test_features)
    joblib.dump(scaler_X, "scripts/data-mining/storage/scaler_X.pkl")
    
    train_y_scaled = scaler_y.fit_transform(train_target)
    test_y_scaled = scaler_y.transform(test_target)
    joblib.dump(scaler_y, "scripts/data-mining/storage/scaler_y.pkl")

    train_combined = train_X_scaled.copy()
    train_combined[:, target_idx] = train_y_scaled.flatten()

    test_combined = test_X_scaled.copy()
    test_combined[:, target_idx] = test_y_scaled.flatten()    

    X_train, y_train = create_sequences_multifeature(train_combined, interval, target_idx)
    X_test, y_test = create_sequences_multifeature(test_combined, interval, target_idx)
    
    return X_train, y_train, X_test, y_test

def main():
    preprocess()
    
if __name__ == "__main__":
    main()