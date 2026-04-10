import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.impute import SimpleImputer
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader

input = input("choose your model: LSTM, BiLSTM or Transformer \n")


class MyLSTM(nn.Module):
    def __init__(self):
        super(MyLSTM, self).__init__()
        self.lstm = nn.LSTM(input_size=43, hidden_size=64, num_layers=2,batch_first=True)#hidden size should be 43 i think...
        self.fc = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32,1)
        )
        self.sigmoid = nn.Sigmoid()
    def forward(self,x):
        out, _ = self.lstm(x)
        last_step_memory = out[:, -1, :]
        predi = self.fc(last_step_memory)
        prob = self.sigmoid(predi)
        return prob

class MyBiLSTM(nn.Module):
    def __init__(self):
        super(MyBiLSTM, self).__init__()
        
        # 1. Turn on the bidirectional flag
        self.lstm = nn.LSTM(
            input_size=43, 
            hidden_size=64, 
            num_layers=2, 
            batch_first=True, 
            bidirectional=True  # <--- NEW
        )
    
        
        # 2. Double the input size of the Linear layer (64 * 2 = 128)
        self.fc = nn.Sequential(
            nn.Linear(128, 32), # <--- NEW (128 instead of 64)
            nn.ReLU(),
            nn.Linear(32, 1)
        )
        self.sigmoid = nn.Sigmoid()
        
    def forward(self, x):
        out, _ = self.lstm(x)
        
        # Grab the combined forward+backward memory from the final timestep
        last_step_memory = out[:, -1, :]
        
        predi = self.fc(last_step_memory)
        prob = self.sigmoid(predi)
        return prob 
    
class SimpleTransformer(nn.Module):
    def __init__(self, input_dim=43, d_model=64, n_heads=4, num_layers=2, seq_len=49):
        super(SimpleTransformer, self).__init__()
        
        # 1. Input Projection: Stretches 44 features to 64 dimensions
        self.input_projection = nn.Linear(input_dim, d_model)
        
        # 2. Positional Encoding: Since all your patients have exactly 49 hours, 
        # we can use a simple learnable parameter to act as our timestamps.
        self.pos_encoder = nn.Parameter(torch.randn(1, seq_len, d_model))
        
        # 3. The Transformer Encoder Block
        # batch_first=True ensures it accepts [Batch, Time, Features]
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, 
            nhead=n_heads, 
            dim_feedforward=128, 
            batch_first=True
        )
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        
        # 4. Final Classification Head
        self.fc = nn.Sequential(
            nn.Linear(d_model, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.input_projection(x)
        x = x + self.pos_encoder    
        x = self.transformer_encoder(x)
        x = x.mean(dim=1)             
        predi = self.fc(x)
        prob = self.sigmoid(predi)
        return prob


def ret_path(imputeds:str, which_dataset:str):
    return f"parquet_files/processedDataProxy-{which_dataset}-{imputeds}.parquet"
path = ret_path("not-imputed", "a")
path_c = ret_path("not-imputed", "c")

raw_df = pd.read_parquet(path=path)
df_c = pd.read_parquet(path = path_c)
y_test = df_c.groupby("PatientID")["In-hospital_death"].first()
y_train = raw_df.groupby("PatientID")["In-hospital_death"].first()
raw_df = raw_df.drop(["In-hospital_death"], axis = 1)
X_test = df_c.drop(["In-hospital_death"], axis = 1)
data_minus_oned = raw_df.set_index("PatientID").groupby("PatientID").ffill().fillna(-1).reset_index()
X_test = X_test.set_index("PatientID").groupby("PatientID").ffill().fillna(-1).reset_index()

sk_imputer = SimpleImputer(missing_values=-1, strategy='median')
sk_imputer.set_output(transform="pandas") 
data_imputed:pd.DataFrame = sk_imputer.fit_transform(data_minus_oned)
X_test = X_test[data_minus_oned.columns]
X_test = sk_imputer.transform(X_test)


n_patients = int(data_imputed.groupby("PatientID").first().count()["Time"])
n_patients_c = int(X_test.groupby("PatientID").first().count()["Time"])
n_hours = int(data_imputed.groupby("PatientID").size().iloc[0]) #is same for test and training

noindex = data_imputed.reset_index()
sorted = noindex.sort_values(by = ["PatientID", "Time"])
features_df = sorted.drop(columns=["PatientID", "Time"])
n_features = features_df.columns.size

X_test = X_test.reset_index().sort_values(by = ["PatientID", "Time"]).drop(columns=["PatientID", "Time"])


Xa_train_np = features_df.values.reshape((n_patients, n_hours, n_features))
X_test = X_test.values.reshape((n_patients_c, n_hours, n_features))
X_test = torch.tensor(X_test, dtype=torch.float32)
Xa_train = torch.tensor(Xa_train_np, dtype=torch.float32)

ya_train = torch.tensor(y_train.values)

Dataset = TensorDataset(Xa_train, ya_train)


dataloader = DataLoader(Dataset, batch_size=30, shuffle=True)
model = MyLSTM()
if(input == "BiLSTM"):
    model = MyBiLSTM() # orrr model = MyBiLSTM() ## orr  SimpleTransformer()
elif(input =="Transformer"):
    model = SimpleTransformer()

criterion = nn.BCELoss()

# The Learner (Adam Optimizer). The 'lr' is the learning rate—how big of a step it takes when learning.
optimizer = optim.Adam(model.parameters(), lr=0.001)

# ==========================================
# 3. THE TRAINING LOOP
# ==========================================
epochs = 10  # How many times to read the whole textbook


for epoch in range(epochs):
    
    # Put the model in "Training Mode" (turns on Dropout)
    model.train() 
    
    total_loss = 0 # Keep track of the total error for this semester
    
    for batch_X, batch_y in dataloader:
        
        # Step 1: Clear the mind (reset gradients)
        optimizer.zero_grad()
        
        # Step 2: Take the test (Forward pass)
        predictions = model(batch_X)
        
        # NOTE: 'predictions' comes out as shape [32, 1]. 'batch_y' is [32].
        # We must use .squeeze() to flatten predictions to [32] so they match perfectly!
        predictions = predictions.squeeze()
        
        # Step 3: Grade the test
        loss = criterion(predictions, batch_y.float())
        
        # Step 4: Learn from mistakes (Backward pass + Optimizer step)
        loss.backward()
        optimizer.step()
        
        # Add up the error so we can print it later
        total_loss += loss.item()
        
    # Calculate the average error across all batches in this epoch
    avg_loss = total_loss / len(dataloader)
    print(f"Epoch {epoch+1}/{epochs} completed. Average Error (Loss): {avg_loss:.4f}")
import torch
from sklearn.metrics import roc_auc_score, average_precision_score

# ==========================================
# 1. GENERATE PREDICTIONS
# ==========================================
model.eval()

with torch.no_grad():
    prediction_probs = model(X_test).squeeze()

# ==========================================
# 2. CONVERT FOR SCIKIT-LEARN
# ==========================================
# prediction_probs is a PyTorch tensor, so we use PyTorch methods to convert it
probs_np = prediction_probs.cpu().numpy()

# y_test is a Pandas Series, so we use Pandas methods to convert it
y_test_np = y_test.to_numpy() 

# ==========================================
# 3. CALCULATE AND OUTPUT SCORES
# ==========================================
auroc = roc_auc_score(y_test_np, probs_np)
ap = average_precision_score(y_test_np, probs_np)

print("=== MODEL PERFORMANCE ===")
print(f"AUROC Score:           {auroc:.4f}")
print(f"Average Precision (AP): {ap:.4f}")


