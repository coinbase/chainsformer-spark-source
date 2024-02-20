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
./target/chainsformer-spark-source-1.0-SNAPSHOT-shaded.jar
```

1. Upload the binary to the DBFS on databricks.
2. Install the binary on the spark cluster.
3. Write spark jobs following examples below.

# Usage
## Batch
```
schema_name = "YOUR_SCHEMA"
in_table = "transactions"
out_table = f"chainsformer_{blockchain}_{network}_{in_table}"
checkpoint_location = f"FileStore/users/{schema_name}/checkpoints/chainsformer/{out_table}"

spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 100)
df = (spark.read.format("coinbase.chainsformer.apache.arrow.flight.spark")
    .option("host", "ethereum-mainnet-prod.coinbase.com")
    .option("port", "8000")
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
in_table = "streamed_transactions"
out_table = f"chainsformer_{blockchain}_{network}_{in_table}"
checkpoint_location = f"FileStore/users/{schema_name}/checkpoints/chainsformer/{out_table}"

spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 100)
df = (spark.readStream.format(""coinbase.chainsformer.apache.arrow.flight.spark")
    .option("host", "ethereum-mainnet-prod.coinbase.com")
    .option("port", "8000")
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
1. Make code changes in the chainsformer-spark-source repo.
2. Build the Jar with `make build`.
3. Upload `chainsformer-spark-source-1.0-SNAPSHOT-shaded.jar` to the DBFS.
4. Install the uploaded Jar to the spark cluster.
5. Start your cluster.
6. Attach notebook to the cluster and run the streaming job.
