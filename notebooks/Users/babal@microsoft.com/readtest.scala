// Databricks notebook source
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
import org.apache.spark.sql.functions._   
import org.apache.spark.sql.functions._
import spark.implicits._
import spark.sql
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.aedstoragebb.blob.core.windows.net",
  "V2wj9mxjhpp57D9ZsqsEYdIJJg2QNHjd/JEub2l9cu+Tm4SwvZs97wx0xBZPOec/5UJF+C/iBS1lXfDUJNc+SA==")

// COMMAND ----------

val jsonpath = "wasbs://iotstore@aedstoragebb.blob.core.windows.net/santacruzaedbb/03/2020/09/29/23"

// COMMAND ----------

val df = spark.read.json(jsonpath)

// COMMAND ----------

display(df)

// COMMAND ----------

val dfexp = df.select($"Body", $"EnqueuedTimeUtc", $"SystemProperties.*")

// COMMAND ----------

display(dfexp)

// COMMAND ----------

dfexp.columns

// COMMAND ----------

val decoded_got = dfexp.withColumn("decoded_base64", unbase64(col("Body")).cast("string"))

// COMMAND ----------

display(decoded_got)

// COMMAND ----------

decoded_got.printSchema

// COMMAND ----------

val dffinal = decoded_got.select($"Body", $"EnqueuedTimeUtc", $"connectionAuthMethod" , $"connectionDeviceGenerationId", $"connectionDeviceId", $"connectionModuleId", $"enqueuedTime", $"decoded_base64")

// COMMAND ----------

display(dffinal)

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

 

val seq2 = Seq((1, """[{"width": "291", "height": "311", "position_x": "244", "position_y": "302", "label": "person", "confidence": "0.760254", "timestamp": "1397430986182"}]"""), 
              (2, """[{"width": "291", "height": "311", "position_x": "244", "position_y": "302", "label": "person", "confidence": "0.760254", "timestamp": "1397430986182"}]"""))
val df21 = seq2.toDF("id", "json")
display(df21)

 

val df22 = df21.select($"json", get_json_object($"json", "$[0].width").alias("width"))
display(df22)

// COMMAND ----------

val jsDF = dffinal.select($"decoded_base64",get_json_object($"decoded_base64", "$[0].width").alias("width"))

// COMMAND ----------

display(jsDF)

// COMMAND ----------

val jsDF = dffinal.select($"EnqueuedTimeUtc", $"connectionAuthMethod" , $"connectionDeviceGenerationId", $"connectionDeviceId", $"connectionModuleId", $"enqueuedTime", $"decoded_base64",get_json_object($"decoded_base64", "$.width").alias("width"), get_json_object($"decoded_base64", "$.height").alias("height"),                  get_json_object($"decoded_base64", "$.position_x").alias("position_x"), get_json_object($"decoded_base64", "$.position_y").alias("position_y"),                          get_json_object($"decoded_base64", "$.label").alias("label"), get_json_object($"decoded_base64", "$.confidence").alias("confidence"),                          get_json_object($"decoded_base64", "$.timestamp").alias("timestamp"))

// COMMAND ----------

display(jsDF)

// COMMAND ----------

val jsDF = dffinal.select($"EnqueuedTimeUtc", $"connectionAuthMethod" , $"connectionDeviceGenerationId", $"connectionDeviceId", $"connectionModuleId", $"enqueuedTime", $"decoded_base64",get_json_object($"decoded_base64", "$.width").alias("width"), get_json_object($"decoded_base64", "$.height").alias("height"),                  get_json_object($"decoded_base64", "$.position_x").alias("position_x"), get_json_object($"decoded_base64", "$.position_y").alias("position_y"),                          get_json_object($"decoded_base64", "$.label").alias("label"), get_json_object($"decoded_base64", "$.confidence").alias("confidence"),                          get_json_object($"decoded_base64", "$.timestamp").alias("timestamp"))

// COMMAND ----------

val jsDF = dffinal.select($"connectionDeviceId", $"decoded_base64", get_json_object($"decoded_base64", "$.width").alias("width"), 
                          get_json_object($"decoded_base64", "$.height").alias("height"),
                          get_json_object($"decoded_base64", "$.position_x").alias("position_x"),
                          get_json_object($"decoded_base64", "$.position_y").alias("position_y"),
                          get_json_object($"decoded_base64", "$.label").alias("label"),
                          get_json_object($"decoded_base64", "$.confidence").alias("confidence"),
                          get_json_object($"decoded_base64", "$.timestamp").alias("timestamp"))

// COMMAND ----------

val stringJsonDF = jsDF.select(to_json(struct($"*"))).toDF("devices")

// COMMAND ----------

display(stringJsonDF)

// COMMAND ----------

val jsDF = stringJsonDF.select($"devices.connectionDeviceId", $"devices.decoded_base64", get_json_object($"devices.decoded_base64", "$.width").alias("width"), 
                          get_json_object($"devices.decoded_base64", "$.height").alias("height"),
                          get_json_object($"devices.decoded_base64", "$.position_x").alias("position_x"),
                          get_json_object($"devices.decoded_base64", "$.position_y").alias("position_y"),
                          get_json_object($"devices.decoded_base64", "$.label").alias("label"),
                          get_json_object($"devices.decoded_base64", "$.confidence").alias("confidence"),
                          get_json_object($"devices.decoded_base64", "$.timestamp").alias("timestamp"))

// COMMAND ----------

display(jsDF)

// COMMAND ----------

val df2 = dffinal.select(split(col("decoded_base64"),",").as("objectlist"))

// COMMAND ----------

display(df2.select($"objectlist[''0'']"))