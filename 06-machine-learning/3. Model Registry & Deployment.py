# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run "../includes/Fetch_User_Metadata"

# COMMAND ----------

import numpy as np
import pandas as pd

import mlflow
import mlflow.pyfunc

# COMMAND ----------

# MAGIC %md #Step 0: Setup

# COMMAND ----------

setup_config = run_setup(USERNAME, DATABASE_NAME)
print(setup_config)

# COMMAND ----------

experiment_config = get_or_create_experiment(PROJECT_PATH, 'Plant Status Prediction')
experiment_path = experiment_config['experiment_path']
experiment_id = experiment_config['experiment_id']

mlflow.set_experiment(experiment_path)
displayHTML(f"<h2>Check the experiment at <a href='#mlflow/experiments/{experiment_id}'>#mlflow/experiments/{experiment_id}</a></h2>")

# COMMAND ----------

# MAGIC %md # Step 1: Grab run manually
# MAGIC - Go back to the Experiment
# MAGIC - Click on any Run in it
# MAGIC - Go down to Artifacts
# MAGIC - Click on `model` folder
# MAGIC - Grab Full Path string

# COMMAND ----------

model_path = "dbfs:/databricks/mlflow-tracking/1632425785075329/d71f648677e943dca647ee99502f4c3c/artifacts/model"
model = mlflow.pyfunc.load_model(model_path)

# COMMAND ----------

df = spark.sql(f'select * from {DATABASE_NAME}.training_data').toPandas()
df['device_operation_status_pred'] = model.predict(df)
preds_stats = df.groupby(['device_operational_status', 'device_operation_status_pred']).count()['id'].reset_index()
display(preds_stats)

# COMMAND ----------

# MAGIC %md # Step 2: Use MLFlow APIs
# MAGIC MLFlow APIs allow you to programatically search for runs, filter, and do analysts of your experiments. 
# MAGIC Let's looks at all of the runs where macro_avg of f1 is > 0.5

# COMMAND ----------

all_runs = mlflow.search_runs(experiment_ids=[experiment_id])
display(all_runs)

# COMMAND ----------

best_runs= mlflow.search_runs(
  experiment_ids=[experiment_id],
  filter_string='metrics.macro_avg__f1_score > 0.5',
  order_by=['metrics.macro_avg__f1_score desc']
)
print(f'Found {best_runs.shape[0]} OK runs')

# COMMAND ----------

# This allows us to grab the best run out of MLFlow very easily 
best_run = best_runs.iloc[0]
print(best_run)

# COMMAND ----------

# MAGIC %md # Step 3: Model Registry

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

model_name = f'sensor_status__{USERNAME}'
print(f'Will be using model name: "{model_name}"')

# COMMAND ----------

client.create_registered_model(model_name)
displayHTML(f"<h2>Check the model at <a href='#mlflow/models/{model_name}'>#mlflow/models/{model_name}</a></h2>")

# COMMAND ----------

# MAGIC %md ## 3.1 Register worst model as version 1

# COMMAND ----------

# Now push worst model run to it 
worst_run = mlflow.search_runs(experiment_ids=[experiment_id], order_by=['metrics.macro_avg__f1_score asc']).iloc[0]
print('Our worst run is:')
print(f'\trun_id={worst_run["run_id"]}')
print(f'\tf1_score={worst_run["metrics.macro_avg__f1_score"]}')

mlflow.register_model(
    f'runs:/{worst_run["run_id"]}/model', model_name
)

# COMMAND ----------

# MAGIC %md 
# MAGIC Now go ahead and observe the model in the Model Registry:
# MAGIC - Click "Models" on the left sidebar
# MAGIC - Find your Model (if your username is "yan_moiseev", you should see it as `sensor_status__yan_moiseev`)
# MAGIC - Click on "Version 1"
# MAGIC - Click on "Stage", transition it to "Production"

# COMMAND ----------

production_model = client.get_latest_versions(model_name, ['Production'])[0]
model = mlflow.pyfunc.load_model(production_model.source)

predictions_worst = model.predict(df)

print(predictions_worst)

# COMMAND ----------

# MAGIC %md ## 3.2 Now let's add best run we had and assign "Production" tag to it automatically

# COMMAND ----------

# Now push best model run to it 
best_run = mlflow.search_runs(experiment_ids=[experiment_id], order_by=['metrics.macro_avg__f1_score desc']).iloc[0]
print('Our best run is:')
print(f'\trun_id={best_run["run_id"]}')
print(f'\tf1_score={best_run["metrics.macro_avg__f1_score"]}')

mlflow.register_model(
    f'runs:/{best_run["run_id"]}/model', model_name
)

# COMMAND ----------

client.transition_model_version_stage(name=model_name, version=2, stage='Production')
client.transition_model_version_stage(name=model_name, version=1, stage='Archived')

# COMMAND ----------

# MAGIC %md # Step 4: Deployment Patterns

# COMMAND ----------

# MAGIC %md ## 4.1 Local Inference
# MAGIC Let's ask Model Registry to give us:
# MAGIC - Model named `sensor_status`
# MAGIC - Latest production version
# MAGIC - Use it locally with a Pandas dataframe

# COMMAND ----------

production_model = client.get_latest_versions(model_name, ['Production'])[0]
model = mlflow.pyfunc.load_model(production_model.source)

predictions_best = model.predict(df)

# COMMAND ----------

# let's also compare it against the worst model we have

df_predictions = pd.DataFrame(
  np.array([predictions_worst.reshape(-1), predictions_best.reshape(-1), df['device_operational_status'].values.reshape(-1)]).T,
  columns=['predictions_worst', 'predictions_best', 'true_labels']
)

display(df_predictions.query('predictions_worst != predictions_best'))

# COMMAND ----------

# MAGIC %md ## 4.2 SQL UDF
# MAGIC - Now let's use the same model, except now we want to wrap it around SQL UDF
# MAGIC - This will allow us to do large-scale inference as model will be distributed to all Databricks workers
# MAGIC - In this case, we are not doing sampling, but rather inferring on all of the rows (few million vs few hundred thousands what we used previously)

# COMMAND ----------

spark.udf.register(
  'sensor_status_model', 
  mlflow.pyfunc.spark_udf(spark, model_uri=production_model.source, result_type='string')
)

spark.sql(f'USE {DATABASE_NAME}')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, sensor_status_model(*) as prediction from training_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists predictions;
# MAGIC 
# MAGIC create temporary view predictions as 
# MAGIC   select device_operational_status as label, sensor_status_model(*) as prediction from training_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This may take a while 
# MAGIC select count(*), label, prediction from predictions group by label, prediction

# COMMAND ----------

# MAGIC %md ##4.3 Streaming 
# MAGIC In Databricks, Streaming and Batch have very similar APIs and lots of code can be re-used! 
# MAGIC 
# MAGIC This means that it is easy to switch from batch inference to streaming. We will re-use the same UDF we have registered before.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Let's first create a copy of our bronze table with 1 row in it
# MAGIC drop table if exists bronze_streaming_cp;
# MAGIC 
# MAGIC create table bronze_streaming_cp using delta
# MAGIC as select * from training_data limit 1;
# MAGIC 
# MAGIC select * from bronze_streaming_cp

# COMMAND ----------

# Now let's read it as a Stream
df = spark.readStream.format('delta').table('bronze_streaming_cp')
df.createOrReplaceTempView('bronze_streaming_cp_tmp')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *, device_operational_status as label, sensor_status_model(*) as prediction from bronze_streaming_cp_tmp;

# COMMAND ----------

# Once stream has initialised (you should see exactly one row of data), run function below to generate some data
generate_data(DATABASE_NAME)

# COMMAND ----------

# MAGIC %md ## 4.4 REST API
# MAGIC We won't go through REST API deployments due to time (and other cloud resources) constraints, but in Databricks you have multiple options available to you:
# MAGIC - Cloud-native integrations: Azure ML and Sagemaker
# MAGIC - Build your own REST API
# MAGIC - Databricks-native Serving

# COMMAND ----------

# MAGIC %md 
# MAGIC An example of how model serving may look like is below:
# MAGIC 
# MAGIC ![Model Serving](https://databricks.com//wp-content/uploads/2020/06/blog-mlflow-model-3.gif)
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC If you would like to learn more, here are some useful links:
# MAGIC - Databricks model serving: https://databricks.com/blog/2020/06/25/announcing-mlflow-model-serving-on-databricks.html
# MAGIC - Sagemaker / mlflow integration: https://www.mlflow.org/docs/latest/python_api/mlflow.sagemaker.html
# MAGIC - Azure ML / mlflow integration: https://www.mlflow.org/docs/latest/python_api/mlflow.azureml.html

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5. What's next
# MAGIC Next step is to account for data drift. [This notebook]($./4. Drift Management) will show you how to load models, check for data drift and re-train them if nedeed.

# COMMAND ----------


