<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Development](#development)
- [Usage](#usage)
  - [Batch](#batch)
  - [Micro Batch Streaming](#micro-batch-streaming)
  - [Options](#options)
- [Release](#release)
  - [Testing](#testing)
  - [Latest Version](#latest-version)
  - [Stable Version](#stable-version)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Overview
Forked from https://github.com/rymurr/flight-spark-source
Tailored for the chainsformer flight service use case
To run in spark, it supports two modes out of three supported by spark data source v2
- [x] Batch
- [x] Micro Batch Streaming
- [ ] Continuous Streaming

# Quick Start
Install Java 8:
1. For x64 processors, follow instructions here: https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html
2. For arm64 processors, use Azul JDK as an alternative to the official package:
    ```shell
    brew tap homebrew/cask-versions
    brew install --cask zulu8
    echo "export JAVA_HOME=`/usr/libexec/java_home -v 1.8`" >> ~/.zshrc
    source ~/.zshrc
    java -version
    ```

Install Maven:
```shell
brew install maven
```

Rebuild everything:
```shell
make build
```

# Development
Binary location
```
./target/flight-spark-source-1.0-SNAPSHOT-shaded.jar
```

1. Upload the binary to the DBFS on databricks.
2. Install the binary on the spark cluster.
3. Write spark jobs following examples below.

# Usage
## Batch
```
schema_name = "YOUR_SCHEMA"
project_name = "YOUR_PROJECT"
job_name = "YOUR_JOB"
tier = 0
blockchain = "ethereum"
network = "mainnet"
env = "production"
in_table = "transactions"
out_table = f"chainsformer_{blockchain}_{network}_{in_table}"
checkpoint_location = f"FileStore/users/{schema_name}/checkpoints/chainsformer/{out_table}"

spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 100)
df = (spark.read.format("cdap.org.apache.arrow.flight.spark")
    .option("project_name", project_name)      
    .option("job_name", job_name)
    .option("tier", tier)
    .option("blockchain", blockchain)
    .option("network", network)
    .option("env", env)
    .option("blocks_start_offset", 14000000)
    .option("blocks_end_offset", 15000000)
    .option("blocks_per_partition", 100)
    .option("blocks_max_batch_size", 20000)
    .option("partition_by_size", 100000)
    .load(in_table))
df.write.option("checkpointLocation", checkpoint_location).saveAsTable(f"{schema_name}.{out_table}")
```

## Micro Batch Streaming
```
schema_name = "YOUR_SCHEMA"
project_name = "YOUR_PROJECT"
job_name = "YOUR_JOB"
tier = 0
blockchain = "ethereum"
network = "mainnet"
env = "production"
in_table = "streamed_transactions"
out_table = f"chainsformer_{blockchain}_{network}_{in_table}"
checkpoint_location = f"FileStore/users/{schema_name}/checkpoints/chainsformer/{out_table}"

spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 100)
df = (spark.readStream.format("cdap.org.apache.arrow.flight.spark")
    .option("project_name", project_name)      
    .option("job_name", job_name)
    .option("tier", tier)
    .option("blockchain", blockchain)
    .option("network", network)
    .option("env", env)
    .option("events_start_offset", 4000000)
    .option("events_per_partition", 100)
    .option("events_max_batch_size", 20000)
    .option("partition_by_size", 100000)
    .load(in_table))
df.writeStream.option("checkpointLocation", checkpoint_location).toTable(f"{schema_name}.{out_table}")
```

## Options
1. blocks_start_offset: the start block height (inclusive). **batch and micro batch**
2. blocks_end_offset: the end block height (exclusive). **batch**
3. blocks_per_record: the number of blocks sent in one http2 stream package/ record **batch and micro batch**
4. blocks_per_partition: the number of blocks included in each partition, which maps to delta table file If not enough blocks, use whatever available. **batch and micro batch**
5. blocks_max_batch_size: limit each batch's max blocks. **micro batch**
6. partition_by_size: the size of partition_by_col to partition on. **micro batch**
7. partition_by_col: the col used to partition by. work with partition_by_size **micro batch**
8. compression: compress algo. LZ4_FRAME, ZSTD, or don't set to opt out as default**batch and micro batch**

# Release

## Testing
To test code changes:
1. Open a PR.
2. Trigger a build for `latest` in Codeflow.
3. Search for "ASSETS:" in the build log to locate the jar.
   The jar is named as `flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
4. Deploy the `latest` configuration to `data-shared-dev-use1`.
5. The jar will be uploaded to `s3a://pynest-pex-data-shared-dev-use1/flight-spark-source/latest/flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
6. Update the Spark job config to reference the above URI.
7. Verify the changes in Spark.

## Latest Version
To release the latest version:
1. Open and merge a PR.
2. Deploy the `latest` configuration to `data-shared-prod-use1` in Codeflow.
3. The jars will be uploaded to:
   - `s3a://pynest-pex-data-shared-prod-use1/flight-spark-source/latest/flight-spark-source-1.0-latest-shaded.jar`.
   - `s3a://pynest-pex-data-shared-prod-use1/flight-spark-source/latest/flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
4. The jars can also be found in DBFS:
   - `dbfs:/mnt/pynest-pex-data-shared-prod-use1/flight-spark-source/latest/flight-spark-source-1.0-latest-shaded.jar`
   - `dbfs:/mnt/pynest-pex-data-shared-prod-use1/flight-spark-source/latest/flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
5. If the Spark job is referencing the first URI, bump up the `deployVersion` of the job config to pick up the changes;
   otherwise, update the Spark job config to reference the second URI.

## Stable Version
1. Test and roll out the latest version.
2. Build the `stable` configuration using the verified commit in Codeflow.
   This image isn't built by default to minimize the risk of unintentional modifications to the stable version.
3. Deploy the `stable` configuration to `data-shared-prod-use1` in Codeflow.
4. The jars will be uploaded to:
   - `s3a://pynest-pex-data-shared-prod-use1/flight-spark-source/stable/flight-spark-source-1.0-stable-shaded.jar`.
   - `s3a://pynest-pex-data-shared-prod-use1/flight-spark-source/stable/flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
5. The jars can also be found in DBFS:
   - `dbfs:/mnt/pynest-pex-data-shared-prod-use1/flight-spark-source/stable/flight-spark-source-1.0-stable-shaded.jar`
   - `dbfs:/mnt/pynest-pex-data-shared-prod-use1/flight-spark-source/stable/flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar`.
6. If the Spark job is referencing the first URI, bump up the `deployVersion` of the job config to pick up the changes;
   otherwise, update the Spark job config to reference the second URI.
