import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from tsfresh import extract_features 
from tsfresh.feature_extraction import EfficientFCParameters, MinimalFCParameters
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score
def ret_path(imputeds:str, which_dataset:str):
    return f"parquet_files/processedDataProxy-{which_dataset}-{imputeds}.parquet"
if __name__ == "__main__":
    path = ret_path("not-imputed", "a")

    raw_df = pd.read_parquet(path=path)
    print("read Parquet")
    y_train = raw_df.groupby("PatientID")["In-hospital_death"].first()
    raw_df = raw_df.drop(["In-hospital_death"], axis = 1)
    data_minus_oned = raw_df.set_index("PatientID").groupby("PatientID").ffill().fillna(-1).reset_index()


    sk_imputer = SimpleImputer(missing_values=-1, strategy='median')
    sk_imputer.set_output(transform="pandas") 
    data_imputed:pd.DataFrame = sk_imputer.fit_transform(data_minus_oned)
    print("sklearn.impute imputer")

    extracted_features = extract_features(data_imputed, column_id="PatientID", column_sort="Time", n_jobs=0,default_fc_parameters=MinimalFCParameters())
    # extracted_features now has many many columns
    print(extracted_features.head(), "extracted_features")


    from tsfresh import select_features


    from tsfresh.utilities.dataframe_functions import impute
    extract_features_clean = impute(extracted_features)
    Trainset = select_features(extract_features_clean, y_train)


    print(f"Predictive features kept: {Trainset.shape[1]}")
    #next step is to use the same features for dataset c
    #so extract features for c and then use the same features trained by select features for predicting with a model. 
    path_for_c = ret_path(imputeds="not-imputed", which_dataset="c")
    C = pd.read_parquet(path_for_c)
    y = C.groupby("PatientID")["In-hospital_death"].first()
    C = C.drop(["In-hospital_death"], axis = 1)
    C = C.set_index("PatientID").groupby("PatientID").ffill().fillna(-1).reset_index()
    C = C.reindex(columns=data_minus_oned.columns, fill_value=-1)
    C_imputed = sk_imputer.transform(C)
    C = impute(extract_features(C_imputed, column_id="PatientID", column_sort="Time",n_jobs=0,default_fc_parameters=MinimalFCParameters()))
    Testset = C[Trainset.columns]

    LogisticRegression_model  =  make_pipeline(StandardScaler(),LogisticRegression(max_iter=1000))
    LogisticRegression_model.fit(X = Trainset, y= y_train)
    probs = LogisticRegression_model.predict_proba(X= Testset)[:,1]
    print(roc_auc_score(y_true=y,y_score=probs))
    print(average_precision_score(y_true=y, y_score=probs))





