# Databricks notebook source
dbutils.widgets.text('database_name', 'demo_yan_moiseev')
dbutils.widgets.text('experiment_id', '2585544766864297')

# COMMAND ----------

import numpy as np
import json
import warnings

from xgboost import XGBClassifier
import mlflow.pyfunc
from mlflow.utils.file_utils import TempDir

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report

from hyperopt import Trials, hp, fmin, tpe, STATUS_FAIL, STATUS_OK

warnings.filterwarnings("ignore")

# COMMAND ----------

database_name = dbutils.widgets.get('database_name')
experiment_id = dbutils.widgets.get('experiment_id')

# COMMAND ----------

table_name = 'training_data'
df = spark.sql(f'select * from {database_name}.{table_name}')

version = sql(f'describe history {database_name}.{table_name} LIMIT 1').collect()[0].version
df = spark.sql(f"select * from {database_name}.{table_name}")
display(df)

# COMMAND ----------

df_pd = df.toPandas()
df_pd = df_pd.sort_values(by='reading_time')
df_train, df_test = df_pd[:250000], df_pd[250000:]

# COMMAND ----------

def _get_model_name(model_class):
  out = str(model_class).replace('<class ', '').replace('>', '').replace("'", '')
  return out


def _transform_labels(y, model):
  encoder = model._y_encoder
  out = encoder.transform(y)
  return out


def _get_metrics(classification_report):
  keys_to_save = ['macro avg', 'weighted avg']
  out = {}
  
  for k in keys_to_save:
    for m, v in classification_report[k].items():
      _k = f'{k}__{m}'.replace(' ', '_').replace('-', '_')
      out[_k] = v

  return out

# COMMAND ----------

DEFAULT_FEATURES = ['device_type', 'device_id', 'reading_1', 'reading_2', 'reading_3']
DEFAULT_CATEGORICAL_FEATURES = ['device_type', 'device_id']
LABEL = 'device_operational_status'

import cloudpickle as pickle

class BaseModel(mlflow.pyfunc.PythonModel):
  def __init__(self, model_cls, model_params={}, table_columns=[]):
    self._model_params = model_params
    self._model_obj = model_cls(**model_params)
    
    self._features = DEFAULT_FEATURES
    self._cat_features = DEFAULT_CATEGORICAL_FEATURES
    self._cont_features=[f for f in self._features if f not in self._cat_features]
    
    self._label = 'device_operational_status'
    
    self._table_columns = table_columns

  def fit(self, X, y):
    self._cat_x_encoder = OneHotEncoder(handle_unknown='ignore').fit(X[self._cat_features])
    self._y_encoder = LabelEncoder().fit(y)
    
    _X = self._preprocess_X(X)
    _y = self._preprocess_y(y)
    
    self._model_obj = self._model_obj.fit(X=_X, y=_y)    
    return self
    
  def predict(self, context, X):
    _y = self._predict(X)
    return _y 

  def _predict(self, X):
    #TODO: hide
    if len(X.columns) == len(self._table_columns):
      X.columns = self._table_columns
      
    _X = self._preprocess_X(X)
    _y_num = self._model_obj.predict(_X)
    _y = self._y_encoder.inverse_transform(_y_num)
    return _y
    
  def load_context(self, context):
    with open(context.artifacts['model'], 'rb') as file:
      self._model_obj = pickle.load(file)
      
  def get_label_names(self):
    out = self._y_encoder.classes_
    return out
  
  def _preprocess_X(self, X):
    _X_processed = np.concatenate(
      [X[self._cont_features], self._cat_x_encoder.transform(X[self._cat_features]).todense()],
      axis=1
    )
    return _X_processed
  
  def _preprocess_y(self, y):
    _y_preprocessed = self._y_encoder.transform(y)
    return _y_preprocessed
  
  
  def log_to_mlflow(self):
    with TempDir() as local_artifacts_dir:
      # dumping model
      model_path = local_artifacts_dir.path('model.pkl')
      with open(model_path, 'wb') as m:
        pickle.dump(self._model_obj, m)
      
      # dumping feature encoder
      cat_encoder_path = local_artifacts_dir.path('cat_encoder.pkl')
      with open(cat_encoder_path, 'wb') as m:
        pickle.dump(self._cat_x_encoder, m)
      
      # dumping label encoder
      label_encoder_path = local_artifacts_dir.path('label_encoder.pkl')
      with open(label_encoder_path, 'wb') as m:
        pickle.dump(self._y_encoder, m)
      
      # all of the model subcomponents will need to go here
      artifacts = {
        'model': model_path,
        'cat_encoder': cat_encoder_path,
        'label_encoder': label_encoder_path
      }
      
      mlflow.pyfunc.log_model(
        artifact_path='model', python_model=self, artifacts=artifacts
      )

# COMMAND ----------

def run(df_train, df_test, experiment_id, base_model, model_params, run_name='Single Run - Automated'):
  with mlflow.start_run(run_name=run_name, experiment_id=experiment_id) as mlflow_run:
    mlflow.log_param('model_name', _get_model_name(base_model))
    mlflow.log_param('features', str(DEFAULT_FEATURES))
    mlflow.log_param('label', str(LABEL))
    mlflow.log_params(model_params)
    
    mlflow.log_param('version', version)
    mlflow.log_param('source_table', table_name)
    mlflow.log_param('source_database', database_name)

    model = BaseModel(model_cls=base_model, model_params=model_params, table_columns=df.columns)
    model = model.fit(df_train[DEFAULT_FEATURES], df_train[LABEL])
    model.log_to_mlflow()

    pred_y = model._predict(df_test[DEFAULT_FEATURES])
    pred_y_num = _transform_labels(pred_y, model)
    true_y_num = _transform_labels(df_test[LABEL], model)
    
    label_names = model.get_label_names()

    report_str = classification_report(true_y_num, pred_y_num, target_names=label_names)    
    mlflow.log_text(report_str, 'classification_report.txt')

    report_dict = classification_report(true_y_num, pred_y_num, target_names=label_names, output_dict=True)
    metrics = _get_metrics(report_dict)
    mlflow.log_metrics(metrics)
    
    out = report_dict['weighted avg'] 
    out['run_id'] = mlflow_run.info.run_uuid
    return out

# COMMAND ----------

params = {'n_estimators': 20, 'max_depth': 3, 'gamma': 0.5}
out = run(df_train, df_test, experiment_id, XGBClassifier, params)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(out))
