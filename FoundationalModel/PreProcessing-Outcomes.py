import pandas as pd
import os
import math
import shutil
from tqdm import tqdm



print("hello")


pathToData = "ml4h_data/p1/Outcomes-c.txt"


dataframe = pd.read_csv(pathToData, index_col=0)

print(dataframe.shape)

survive_Hospital = dataframe.iloc[:,[-1]]
print(survive_Hospital.iloc[3])

survive_Hospital.to_csv("processedOutcomes-c.txt")