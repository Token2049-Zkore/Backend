import numpy as np
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier as XgB
import os
import pickle
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score

# evaluation
def evaluation(y_true, y_pred):
    confusion_result = confusion_matrix(y_true, y_pred)
    precision_result = precision_score(y_true, y_pred, average = None, zero_division = 0)
    recall_result = recall_score(y_true, y_pred, average = None, zero_division = 0)
    f1_result = f1_score(y_true, y_pred, average = None, zero_division = 0)
    f1_result_total = f1_score(y_true, y_pred, average = 'macro', zero_division = 0)
    print("confusion matrix\n", confusion_result)
    print("precision score", precision_result)
    print("recall score", recall_result)
    print("f1 score", f1_result)
    print("f1 score of label 2 :", f1_result[2])
    print("f1 total score : ",f1_result_total)

# train_data = pd.read_csv(f"data.csv")
# X, y = train_data.iloc[:,:-1], train_data.iloc[:,-1]

NUM_CLASSES = 3

iris = load_iris()
X, y = iris.data, iris.target
X = X.astype(np.float32)

# split to traininig data and validation data
X_train, X_valid, y_train, y_valid = train_test_split(X, y)
clr =  XgB(n_estimators = 30)
clr.fit(X_train, y_train)

# evaluation
y_pred = clr.predict(X_valid)

evaluation(y_valid, y_pred)

# save training model
save_path = "./model.pkl"
with open(save_path, 'wb') as f:
    pickle.dump(clr, f)