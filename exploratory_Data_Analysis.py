import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Load the data
print("Loading Parquet file...")
df = pd.read_parquet("processedDataProxy.parquet")

# 2. Let's pick 4 variables that show completely different behaviors
# HR (Heart Rate), Bilirubin (Liver enzyme), Gender, Age
vars_to_plot = ['HR', 'Bilirubin', 'Gender', 'Age']

# Create a 2x2 grid of charts
plt.figure(figsize=(12, 8))

for i, var in enumerate(vars_to_plot):
    plt.subplot(2, 2, i+1)
    
    # We drop the NaNs just for the plot, otherwise it won't draw correctly
    data_to_plot = df[var].dropna()
    
    if var == 'Gender':
        # Gender is categorical, so we use a bar chart (countplot)
        sns.countplot(x=data_to_plot, palette="Set2")
        plt.title(f"Count of {var} (Categorical Code)")
    else:
        # For continuous numbers, we use a histogram
        sns.histplot(data_to_plot, kde=True, color='skyblue', bins=40)
        plt.title(f"Distribution of {var}")

plt.tight_layout()
plt.show()

# 3. Check for unique values to answer the "numerical vs codes" question
print("\n--- Unique Values Check ---")
print(f"Unique values in Gender: {df['Gender'].dropna().unique()}")
print(f"Unique values in In-hospital_death: {df['In-hospital_death'].dropna().unique()}")