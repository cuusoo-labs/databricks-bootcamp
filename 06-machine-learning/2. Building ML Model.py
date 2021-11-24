# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run "../Lab 1: Data Engineering/Utils/Fetch_User_Metadata"

# COMMAND ----------

# MAGIC %md #Step 0: Run setup code

# COMMAND ----------

setup_config = run_setup(USERNAME, DATABASE_NAME)
print(setup_config)

# COMMAND ----------

df = spark.read.table(f"{DATABASE_NAME}.training_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Create an Experiment
# MAGIC Here we are creating a Machine Learning experiment through an `mlflow.create_experiment` API call. [Link to docs to learn more](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.create_experiment).
# MAGIC 
# MAGIC It is also possible to create an experiment through a UI. To do that, click on:
# MAGIC - Workspace button on a panel to the left.
# MAGIC - Right click on an empty space -> Create -> MLFlow Experiment. 
# MAGIC - Give it a name, optionally change Experiment Artifact location. 

# COMMAND ----------

experiment_config = get_or_create_experiment(PROJECT_PATH, 'Plant Status Prediction')
experiment_path = experiment_config['experiment_path']
experiment_id = experiment_config['experiment_id']

mlflow.set_experiment(experiment_path)
displayHTML(f"<h2>Make sure you can see your experiment on <a href='#mlflow/experiments/{experiment_id}'>#mlflow/experiments/{experiment_id}</a></h2>")

# COMMAND ----------

# MAGIC %md #Step 2: Building Model Wrapper
# MAGIC 
# MAGIC In this lab, we will be using `pyfunc` MLFlow flavour. It is the most flexible MLFlow flavour that is suitable for cases where your model users multiple libraries or custom parts. 
# MAGIC 
# MAGIC `mlflow.pyfunc.log_model` requires two things:
# MAGIC   - [1] Instance of `mlflow.pyfunc.PythonModel` object that implements `load_context()` and `predict()` methods. Think of it as a model wrapper that can assemble your model subcomponents into a model. This is very useful when your model subcomponents have different serialisation methods. For example, some artefacts may be saved as json, pickle, while other require H5 files (such as Keras)
# MAGIC   - [2] An `artifacts` dictionary that contain filepaths to various subcomponents of your model. This artifact dictionory will be available to your `load_context()` and `predict()` methods.

# COMMAND ----------

import pickle

import mlflow.pyfunc
from mlflow.utils.file_utils import TempDir

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder

# COMMAND ----------

DEFAULT_FEATURES = ['device_type', 'device_id', 'reading_1', 'reading_2', 'reading_3', 'mean_5m_reading_1', 'mean_5m_reading_2', 'mean_5m_reading_3']
DEFAULT_CATEGORICAL_FEATURES = ['device_type', 'device_id']
LABEL = 'device_operational_status'

import cloudpickle as pickle
import numpy as np

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

# MAGIC %md # Step 3: Running & logging ML Model

# COMMAND ----------

from sklearn.metrics import classification_report

# COMMAND ----------

TABLE_NAME = 'training_data'
df = spark.sql(f"select * from {DATABASE_NAME}.{TABLE_NAME}")
display(df)

df_pd = df.toPandas()
df_pd = df_pd.sort_values(by='reading_time')
df_train, df_test = df_pd[:250000], df_pd[250000:]
VERSION = sql(f'DESCRIBE HISTORY {DATABASE_NAME}.{TABLE_NAME} LIMIT 1').collect()[0].version

# COMMAND ----------

# MAGIC %md ## 3.1 Train-test-measure loop

# COMMAND ----------

def run(df_train, df_test, experiment_id, base_model, model_params, run_name='Single Run - Manual'):
  with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    mlflow.log_param('model_name', _get_model_name(base_model))
    mlflow.log_param('features', str(DEFAULT_FEATURES))
    mlflow.log_param('label', str(LABEL))
    mlflow.log_param('version', VERSION)
    mlflow.log_param('source_table', TABLE_NAME)
    mlflow.log_param('source_database', DATABASE_NAME)
    mlflow.log_params(model_params)

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
    return out

# COMMAND ----------

# MAGIC %md ## 3.2 Try different parameters with RidgeClassifier
# MAGIC ![test](https://upload.wikimedia.org/wikipedia/commons/thumb/0/05/Scikit_learn_logo_small.svg/440px-Scikit_learn_logo_small.svg.png)

# COMMAND ----------

from sklearn.linear_model import RidgeClassifier

# COMMAND ----------

params = {'alpha': 1.0, 'fit_intercept': True, 'normalize': False}
out = run(df_train, df_test, experiment_id, RidgeClassifier, params)
print(out)

# COMMAND ----------

params = {'alpha': 1.0, 'fit_intercept': True, 'normalize': True}
out = run(df_train, df_test, experiment_id, RidgeClassifier, params)
print(out)

# COMMAND ----------

params = {'alpha': 0.5, 'fit_intercept': False, 'normalize': True}
out = run(df_train, df_test, experiment_id, RidgeClassifier, params)
print(out)

# COMMAND ----------

# MAGIC %md ##3.3 Looks like RidgeRegressor is a bad choice... let's try XGBoost
# MAGIC ![my_test_image](https://upload.wikimedia.org/wikipedia/commons/6/69/XGBoost_logo.png)

# COMMAND ----------

from xgboost import XGBClassifier

# COMMAND ----------

params = {'n_estimators': 20, 'max_depth': 1, 'gamma': 0.5}
out = run(df_train, df_test, experiment_id, XGBClassifier, params)
print(out)

# COMMAND ----------

# MAGIC %md ##3.4 Finding best parameters with HyperOpt

# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image](https://www.jeremyjordan.me/content/images/2017/11/grid_search.gif)
# MAGIC ![my_test_image](https://www.jeremyjordan.me/content/images/2017/11/Bayesian_optimization.gif)

# COMMAND ----------

from hyperopt import Trials, hp, fmin, tpe, STATUS_FAIL, STATUS_OK

# This is our search space
params = {
  'n_estimators': hp.choice('n_estimators', [i for i in range(1, 15)]),
  'max_depth': hp.choice('max_depth', [1, 2, 3]),
  'gamma': hp.uniform('gamma', 0.01, 1.0),
  'verbosity': hp.choice('verbosity', [0])
}


# We want to minimise loss: in this case, it's f1-score
def fmin_objective(params):
  metrics = run(df_train, df_test, experiment_id, XGBClassifier, params, 'HyperOpt - Manual')
  loss = 1 - metrics['f1-score']
  out = {'loss': loss, 'status': STATUS_OK}
  
  return out

# COMMAND ----------

best_param = fmin(
  fn=fmin_objective, 
  space=params, 
  algo=tpe.suggest, 
  max_evals=20, 
  trials=Trials()
) 

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: What's next
# MAGIC Now that we know how to train our models, let's go to the next [notebook]($./3. Model Registry & Deployment) to find out how to deploy them in production

# COMMAND ----------


