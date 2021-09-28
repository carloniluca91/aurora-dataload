# Aurora Dataload

`Scala` project that implements a `Spark` ingestion engine that reads
batch files from `HDFS` and save processed data to some `Hive` tables 

The engine takes care of reading a `json` file located on HDFS and ingest
files according to the specifications stated in such file. Among these specifications are

* filters to apply to raw data
* transformations to apply to raw data
* information for duplicates removal and data partitioning

Processed data are then stored into Hive. Right after that, an `ImpalaQl` statement 
is triggered in order to make stored data immediately available to `Impala` for analysis 
and querying purposes. In addition, the engine moves each ingested file to a different HDFS 
location and stores a record on a Hive table for logging purposes

This project is a personal, free-time rework of a real life project
I had extensively worked on during my career ;)

Main modules

* `application` which represents the entry point
* `core` which defines core classes and implicits
* `configuration` which defines models for application's configuration

Some details

* written in Scala `2.11.12`
* developed, tested and deployed on a `CDH 6.3.2` environment (`Spark 2.4.0`)
* Spark application is submitted by means of a `bash` script (originally scheduled using Control-M) which can be found within `bash` folder at project's root