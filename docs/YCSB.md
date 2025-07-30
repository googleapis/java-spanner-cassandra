# YCSB

Follow these steps to initialize and run benchmarks with [ycsb](https://github.com/brianfrankcooper/YCSB) with Spanner Cassandra Java Client.

For best results, we strongly recommend running the benchmark from a Google Compute Engine (GCE) VM.

## Set up Cloud Spanner with the Expected Schema

Create a spanner table with below schema:

```sql
CREATE TABLE
  usertable (y_id STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field0 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field1 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field2 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field3 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field4 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field5 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field6 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field7 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field8 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    field9 STRING(MAX) OPTIONS ( cassandra_type = 'varchar' ),
    )
PRIMARY KEY (y_id);
```

## Set Up Your Environment and Auth

You need to install all the necessary dependencies to run [java-spanner-cassandra](https://github.com/googleapis/java-spanner-cassandra) and YCSB script (below examples are for Debian-based GCE VM).

```shell
// install git
sudo apt install git-all

// install jdk (jdk 17 works)
sudo apt install openjdk-17-jdk

// update packages
sudo apt update

// GCE VM should already have python3 installed,YCSB uses python so we need to create a symbolic link from python3 to python. If you already have python installed you can skip this step.
sudo ln -s /usr/bin/python3 /usr/bin/python

// install maven
sudo apt install maven
```

Follow the [set up instructions](https://cloud.google.com/spanner/docs/getting-started/set-up) in the Cloud Spanner documentation to set up your environment and authentication. When not running on a GCE VM, make sure you run `gcloud auth application-default login`.

## Load the Data

To load the data, first clone and check out to this [slightly modified YCSB branch](https://github.com/ShuranZhang/YCSB/tree/c2sp) that contains some necessary compatibility changes to work with spanner-cassandra clients: 

```shell
git clone https://github.com/ShuranZhang/YCSB.git
cd YCSB
git checkout c2sp
```

Run the following command to load 100 million rows into your `usertable`. We use the standard Spanner driver for the data load phase because it's more efficient for bulk inserts. Make sure to replace the placeholder values with your specific Google Cloud project, Spanner instance, and database information.

```shell
./bin/ycsb load cloudspanner  -P cloudspanner/conf/cloudspanner.properties  -P workloads/workloadb  -p recordcount=1000000000  -p insertorder=ordered  -p cloudspanner.project=your-project  -p cloudspanner.instance=your-instance  -p cloudspanner.database=your-database  -p cloudspanner.batchinserts=1000  -p cloudspanner.channels=60  -threads 128 -s > load_100m.log 2>&1
```

## Run a workload

Before starting to run any YCSB scripts, make sure you clone and start a java-spanner-cassandra process as a sidecar proxy(see [complete instructions](https://github.com/googleapis/java-spanner-cassandra?tab=readme-ov-file#sidecar-proxy-or-standalone-process)).

```shell
// clone latest java client source code
git clone https://github.com/googleapis/java-spanner-cassandra.git 

// cd to the core package
cd java-spanner-cassandra/google-cloud-spanner-cassandra

// build
mvn clean install -DskipTests

// set number of YCSB threads to generate traffic and nummber of TCP connections to open. // The number of grpc channels needed for proxy will be roughly THREADS/2.
export THREADS=10

// run sidecar launcher on localhost:9042 by default
java -DdatabaseUri=projects/my-project/instances/my-instance/databases/my-database -DnumGrpcChannels=$(($THREADS / 2)) -jar target/spanner-cassandra-launcher.jar
```

After the sidecar proxy has been successfully running on your machine, you can start the actual YCSB test script. Below is a sample script that runs workload b(5% update, 95% read) and dumps the result or any errors into a dedicated log file. Other benchmarks use different read/write proportions. You can find the YCSB stats numbers dumped in the log file

```shell
// test time in seconds 
export EXECUTION_TIME=600

TZ=":America/Los_Angeles" date && time ./bin/ycsb run cassandra-cql   -P workloads/workloadb   -p hosts=localhost   -p requestdistribution=zipfian   -p maxexecutiontime=$EXECUTION_TIME   -p operationcount=1000000000   -p cassandra.maxconnections=$THREADS   -p cassandra.coreconnections=$THREADS -threads $THREADS -s > b_threads${THREADS}_${EXECUTION_TIME}s.log 2>&1
```

>> Important note: We highly recommend to set `cassandra.coreconnections` and `cassandra.maxconnections` same with the number of YCSB threads. Increasing these values enables more concurrent connections between the client and Cassandra Interface. This can prevent connection pool exhaustion under heavy load.
