# Setting up AED devices and process data output

## Architecture

![alt text](https://github.com/balakreshnan/AED/blob/main/images/aedarch.jpg "Architecture")

## Setup the Device

- Open the box
- Assemble the kit
- connect the proper cables needed
- Start the device
- Connect to the AED wifi access point starts with SFD_xxxxxx
- Open http://IP:4242 and it would take you to guided setup
- Set the name and iot hub name
- for DPS just use the iot hub name
- Go to Project Santa clara web site and push people count example.
- Takes a while so wait it will install like 8 modules

![alt text](https://github.com/balakreshnan/AED/blob/main/images/allmodules.jpg "All modules")

## Data processing in Cloud

- Eye module identifies identifies people and send it to IoT Hub
- Enable Routing messages to store the data in ADLS gen2 storage
- Routing stores the message as base 64 encoded
- Please decode to view the actual data
- create another endpoint to send that to stream analytics
- Configure stream analytics to save the data in blob for now

## Data processing in Azure Data bricks for data stored as message routing

- Here is the code
- First imports

```
import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
import org.apache.spark.sql.functions._   
import org.apache.spark.sql.functions._
import spark.implicits._
import spark.sql
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
```

- now set the storage config

```
spark.conf.set(
  "fs.azure.account.key.accountname.blob.core.windows.net",
  "storagekey")
```

- now the path where is stores usually year/month/day/hour/minute

```
val jsonpath = "wasbs://iotstore@aedstoragebb.blob.core.windows.net/aedbb/03/2020/09/29/23"
```

- Read the json and display

```
val df = spark.read.json(jsonpath)
display(df)
```

- Expand system propertises

```
val dfexp = df.select($"Body", $"EnqueuedTimeUtc", $"SystemProperties.*")
display(dfexp)
```

- now the important step to decode the Body (actual content)

```
val decoded_got = dfexp.withColumn("decoded_base64", unbase64(col("Body")).cast("string"))
display(decoded_got)
```

- now select necessary columns

```
val dffinal = decoded_got.select($"Body", $"EnqueuedTimeUtc", $"connectionAuthMethod" , $"connectionDeviceGenerationId", $"connectionDeviceId", $"connectionModuleId", $"enqueuedTime", $"decoded_base64")
display(dffinal)
```

- now parse the JSON message for values predicted

```
val jsDF = dffinal.select($"EnqueuedTimeUtc", $"connectionAuthMethod" , $"connectionDeviceGenerationId", $"connectionDeviceId", $"connectionModuleId", $"enqueuedTime", $"decoded_base64",get_json_object($"decoded_base64", "$.width").alias("width"), get_json_object($"decoded_base64", "$.height").alias("height"),                  get_json_object($"decoded_base64", "$.position_x").alias("position_x"), get_json_object($"decoded_base64", "$.position_y").alias("position_y"),                          get_json_object($"decoded_base64", "$.label").alias("label"), get_json_object($"decoded_base64", "$.confidence").alias("confidence"),                          get_json_object($"decoded_base64", "$.timestamp").alias("timestamp"))
```

```
display(jsDF)
```

More to come.