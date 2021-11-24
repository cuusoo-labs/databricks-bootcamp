# Databricks notebook source
# MAGIC %md # 1 Context
# MAGIC 
# MAGIC Building upon Data Engineering & Exploratory Data Analytics work we have done, we are now tasked with creating a system that can predict failures of our IoT devices. 
# MAGIC 
# MAGIC To do that, we need to go through three main notebooks: 
# MAGIC - [1. Feature Store.]($./1. Feature Store) In this notebook, you will learn how to use Databricks Feature Store to register feature and create your training dataset.
# MAGIC - [2. Building ML Model.]($./2. Building ML Model) In this notebook, you will learn how to build multiple Machine Learning models & track them with MLFlow Tracking Server. We will capture various useful attributes of those models such as metrics, hyperparameters and models themselves. We will also learn how to use some of the Databricks-native libraries such as HyperOpt for a large scale automated Hyperparameter Tuning.
# MAGIC - [3. Model Registry & Deployment.]($./3. Model Registry & Deployment) Continuing from the previous lab, in this notebook you will learn how to take models you trained & make them available to your organisation. We will expole the concept of Model Registry, model versioning & various deployment patterns that you can use to operationalise your Machine Learning models. 
# MAGIC - [4. (OPTIONAL) Drift Management.]($./4. Drift Management) In this session we will simulate Data Drift to observe how changing data may affect our models. We will learn some basic methods of how to work with Data Drift & how you can automate it with MLFlow Model Registry & Databricks Delta. 

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 MLFlow Process Flow overview
# MAGIC ![mlflow-overview](https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg)
