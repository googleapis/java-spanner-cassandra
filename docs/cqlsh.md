# Integrating cqlsh with Spanner Cassandra Java Client

This guide provides detailed instructions on setting up `cqlsh` to connect to the Spanner Cassandra Java Client.

## Prerequisites

- **cqlsh Version:** Ensure you have `cqlsh` versions from the branches `cassandra-4.0.13` or `cassandra-4.1.5`.
- **Spanner Cassandra Java Client Setup:** Ensure the Spanner Cassandra Java Client is set up and running.

## Setup Instructions

>> NOTE: You may skip these instructions, if you already have `cqlsh` interface installed.

### Option 1: Install cqlsh Directly

#### Step 1: Download cqlsh

Download the appropriate version of `cqlsh` by cloning the repository and following the instructions:

- **cqlsh 4.0.13:**
  ```sh
  git clone https://github.com/apache/cassandra.git -b cassandra-4.0.13
  cd cassandra/bin
  ```

- **cqlsh 4.1.5:**
  ```sh
  git clone https://github.com/apache/cassandra.git -b cassandra-4.1.5
  cd cassandra/bin
  ```

#### Step 2: Configure cqlsh to Connect to the Spanner Cassandra Java Client (Optional)

Edit the `cqlsh` configuration to point to the Spanner Cassandra Java Client:

1. Open the `cqlshrc` configuration file. If it does not exist, create one in your home directory:
   ```sh
   nano ~/.cassandra/cqlshrc
   ```

2. Add the following configuration:
   ```ini
   [connection]
   hostname = <java_client_hostname>
   port = <java_client_port>
   ```

   Replace `<java_client_hostname>` and `<java_client_port>` with the appropriate values for your java client setup.

#### Step 3: Launch cqlsh

Launch `cqlsh` with the configured/default settings:
```sh
./cqlsh --protocol-version 4
```

Launch `cqlsh` with the custom hostname and port:
```sh
./cqlsh <java_client_hostname> <java_client_port> --protocol-version 4
```

Replace `<java_client_hostname>` and `<java_client_port>` with the appropriate values.

### Option 2: Use Dockerized cqlsh

#### Step 1: Install Docker

Ensure Docker is installed on your machine. Follow the instructions on the [Docker website](https://docs.docker.com/get-docker/) to install Docker for your operating system.

#### Step 2: Download and Run the Dockerized cqlsh

Download the relevant Docker image and open a bash shell:
```sh
docker run -it nuvo/docker-cqlsh bash
```

#### Step 3: Find Your Machine’s IP Address

Find the local IP address of the machine if the java client is running locally. For macOS, you can get the local machine IP address using:
```sh
ifconfig | grep "inet " | grep -v 127.0.0.1
```

#### Step 4: Connect to the Spanner Cassandra Java Client

Open a bash shell in the Docker image:
```sh
docker run -it nuvo/docker-cqlsh bash
```

Connect to the Spanner Cassandra Java Client using your IP address and port:
```sh
cqlsh --protocol-version 4 '<your_ip_address>' <port>
```

Replace `<your_ip_address>` with the local IP address and `<port>` obtained in Step 3.

### Option 3: Use pip

To download and install `cqlsh` using `pip`, the Python package installer, one can use the following command:

```sh
pip install cqlsh
```

## Basic CRUD Operations

**Insert:**
```sql
INSERT INTO keyspace_name.table_name (col1, col2, time, count) VALUES ('1234', 'check', '2024-06-13T05:19:16.882Z', 10);
```

**Select:**
```sql
SELECT * FROM keyspace_name.table_name WHERE col1 = '1234';
```

**Update:**
```sql
UPDATE keyspace_name.table_name SET count = 15 WHERE col1 = '1234' AND col2 = 'check' AND time = '2024-06-13T05:19:16.882Z';
```

**Delete:**
```sql
DELETE FROM keyspace_name.table_name WHERE col1 = '1234' AND col2 = 'check';
```

## Unsupported Queries

DDL queries are not supported by the Spanner Cassandra Java Client when using `cqlsh`:

- **Create Table:**
  ```sql
  CREATE TABLE keyspace_name.table_name (id UUID PRIMARY KEY, name text);
  ```

- **Drop Table:**
  ```sql
  DROP TABLE keyspace_name.table_name;
  ```

- **Describe Table:**
  ```sql
  DESCRIBE TABLE keyspace_name.table_name;
  ```

