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
val jsonpath = "wasbs://containername@storagename.blob.core.windows.net/aedbb/03/2020/09/29/23"
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

## Processing Data using Azure Stream Analytics

- Setup Stream analytics
- Create input as iot hub 
- i created a consumer group for iot hub to store the data
- Create a output as blob storage json is good
- Write the below query

```
SELECT
    width,
    height,
    position_x,
    position_y,
    label,
    confidence,
    timestamp,
    EventProcessedUtcTime,
    EventEnqueuedUtcTime,
    IoTHub.ConnectionDeviceId as ConnectionDeviceId,
    IoTHub.ConnectionDeviceGenerationId as ConnectionDeviceGenerationId,
    IoTHub.EnqueuedTime as EnqueuedTime 
INTO
    outputblob
FROM
    input
```

- Start the job
- Let it run for few mins and check the output
- Sample output

```
{"width":"303","height":"413","position_x":"253","position_y":"203","label":"person","confidence":"0.618164","timestamp":"1602006050538829102","EventProcessedUtcTime":"2020-10-06T17:42:26.8677287Z","EventEnqueuedUtcTime":"2020-10-06T17:40:50.0830000Z","ConnectionDeviceId":"device1","ConnectionDeviceGenerationId":"637370139831812385","EnqueuedTime":"2020-10-06T17:40:49.0000000"}
{"width":"328","height":"400","position_x":"262","position_y":"216","label":"person","confidence":"0.513672","timestamp":"1602006060996325239","EventProcessedUtcTime":"2020-10-06T17:42:26.9146205Z","EventEnqueuedUtcTime":"2020-10-06T17:41:00.6130000Z","ConnectionDeviceId":"device1","ConnectionDeviceGenerationId":"637370139831812385","EnqueuedTime":"2020-10-06T17:41:00.0000000"}
{"width":"325","height":"391","position_x":"257","position_y":"225","label":"person","confidence":"0.590332","timestamp":"1602006062746189626","EventProcessedUtcTime":"2020-10-06T17:42:26.9146205Z","EventEnqueuedUtcTime":"2020-10-06T17:41:02.2690000Z","ConnectionDeviceId":"device1","ConnectionDeviceGenerationId":"637370139831812385","EnqueuedTime":"2020-10-06T17:41:02.0000000"}
{"width":"319","height":"393","position_x":"263","position_y":"223","label":"person","confidence":"0.627441","timestamp":"1602006064496043522","EventProcessedUtcTime":"2020-10-06T17:42:26.9146205Z","EventEnqueuedUtcTime":"2020-10-06T17:41:04.1130000Z","ConnectionDeviceId":"device1","ConnectionDeviceGenerationId":"637370139831812385","EnqueuedTime":"2020-10-06T17:41:03.0000000"}
{"width":"326","height":"404","position_x":"263","position_y":"212","label":"person","confidence":"0.507812","timestamp":"1602006066204242228","EventProcessedUtcTime":"2020-10-06T17:42:27.4006536Z","EventEnqueuedUtcTime":"2020-10-06T17:41:05.7420000Z","ConnectionDeviceId":"device1","ConnectionDeviceGenerationId":"637370139831812385","EnqueuedTime":"2020-10-06T17:41:05.0000000"}
```

- now with the above data we can go about doing anything in cloud.

More to come.