# Databricks notebook source
location = "dbfs:/FileStore/shared_uploads/rajendraku0000@gmail.com/New_Text_Document-1.txt"

# COMMAND ----------

df=sc.textFile(location).map(lambda x:x.split("~|")).toDF(schema=["Name","Age"])
display(df)


# COMMAND ----------

df=df.filter(df["Name"] != "Name")
display(df)
