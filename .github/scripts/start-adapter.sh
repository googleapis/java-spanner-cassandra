#!/bin/bash

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script starts the Spanner Cassandra Adapter and waits for it to be healthy.

# Exit immediately if a command exits with a non-zero status, and also exit if any command in a pipeline fails.
set -eo pipefail

# Find the launcher JAR file.
LAUNCHER_JAR=$(find google-cloud-spanner-cassandra/target -name "spanner-cassandra-launcher.jar")
if [ -z "$LAUNCHER_JAR" ]; then
  echo "Error: spanner-cassandra-launcher.jar not found."
  exit 1
fi

# Base parameters for the adapter.
ADAPTER_PARAMS="-Dhost=127.0.0.1 -Dport=9042 -DhealthCheckPort=8080 -DdatabaseUri=$NOSQLBENCH_DATABASE_URI"

# Add the Spanner endpoint parameter only for the 'devel' environment.
# The MATRIX_ENVIRONMENT variable is passed from the GitHub Actions workflow.
if [ "$MATRIX_ENVIRONMENT" = "devel" ]; then
  ADAPTER_PARAMS="$ADAPTER_PARAMS -DspannerEndpoint=$SPANNER_ENDPOINT"
fi

echo "Starting adapter with params: $ADAPTER_PARAMS"
# Start the adapter in the background (&) so the workflow can proceed.
java $ADAPTER_PARAMS -jar $LAUNCHER_JAR &

# Health check logic.
echo "Waiting for adapter to start..."
timeout=60
# Poll the /debug/health endpoint until it returns a 200 OK status.
# The `curl -sf` command fails silently (returns a non-zero exit code) on HTTP errors,
# causing the loop to continue until a successful response is received.
while ! curl -sf http://127.0.0.1:8080/debug/health > /dev/null; do
  # Fail the workflow if the adapter doesn't start within the timeout period.
  if [ $timeout -le 0 ]; then
    echo "Adapter did not start within 60 seconds."
    exit 1
  fi
  sleep 5
  timeout=$((timeout - 5))
done

echo "Adapter started successfully."
