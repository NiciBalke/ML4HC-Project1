# ML4HC
## 1. Data Processing and Exploration (5 Pts)
### Q1.1: Data Transformation (1 Pts)
run DataPreProcessing.py to get parquet files containing the data later used for several methods. reads data, concatinates different patients files, rounds up the timesteps if necessary and imputes it.


### Q1.2 Exploratory Data Analysis (2 Pts)
run EDA.ipynb, where you can alter the range of age or other variables to see their imact on the distribution of the data

### Q1.3 Preprocess data for Machine Learning (2 Pts)
Implemented in DataPreprocessing.py (different types of imputation) and in supervised_methods_clean.py (Standard Scaler), also tested RobustScaler in supervised_methods.ipynb

## 2. Supervised Learning (16 Pts)
### Q2.1 Classic Machine Learning Methods (5 Pts)
RandomForests and LogisticRegression tested in supervised_methods.ipynb
tested mean, max, min in supervised_methods.ipynb
from here (https://tsfresh.readthedocs.io/en/latest/) also some other features were tested in supervised_methods.clean

### Q2.2 Recurrent Neural Networks (4 Pts) 
run LSTM_clean.py and decide on LSTM, Bidirectional LSTM and Transformer models

### Q2.3a: Transformers (3 Pts)
run LSTM_clean.py with Transformer model

### Q2.3b: Tokenizing Time-Series Data and Transformers (4 Pts)
run RePreprocessingData.ipynb

## 3. Representation Learning
The code for all of task 3 is contained in the file auto_encoder_base.py
To run the best model for each of the model types on a cluster, use the batch files:
 - batch_best_auto.sh
 - batch_best_constrastive.sh
 - batch_best_hybrid.sh
 
To run all three of these use:
 - batch_best_all.sh

To run a wandb sweep, set the API Key and run using:
 - export WANDB_API_KEY=
 - batch_sweep.sh 

The yaml files contain the best hyperparameters for each of the models found during exploration. Plots for visualizations will be saved under embeddings/ and logs will be produced in logs/.

## 4. Foundation Models
### Q4.1
Run llama_predict.py
predict_mode = 0 only runs score computation and uses a prediction.txt file which has to be present

in predict_mode = 1 && embed_mode = 0: llama is used to make predictions.
in predict_mode = 1 && embed_mode = 1: embeddings are extracted from llama and saved.

### Q4.2
multiple linear_probes are available (logistic regression was the best)
Run any of logistic_regression.py/ridge_regression.py/svm.py/random_forest.py all of them require files with embeddings to be present in the embeddings folder

### Q4.3
Run chronos_embed.py to generate embeddings
then again run any of the linear probes above




