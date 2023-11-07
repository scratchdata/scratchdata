#!/bin/bash

set -eo pipefail;

# Target directory
TMPL_DIR="/templates/";
CONFIG_DIR="/etc/clickhouse-server/config.d/";

# Check if the directory exists
if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "directory $CONFIG_DIR does not exist!";
  exit 1;
fi

# Check if the variables are set
varlist=("CLUSTER" "SERVER_ID" "REPLICA_ID" "SECRET" "SHARD_ID");
for var in "${varlist[@]}"; do
  if [[ -z "${!var}" ]]; then
    echo "Error: Variable $var is not set.";
    exit 1;
  fi
done

for file in "$TMPL_DIR"*; do
  # Ensure it is a file and not a subdirectory
  if [[ -f $file ]]; then
    filename=$(basename "$file")

    sed \
      -e "s/\$CLUSTER/$CLUSTER/g" \
      -e "s/\$SERVER_ID/$SERVER_ID/g" \
      -e "s/\$REPLICA_ID/$REPLICA_ID/g" \
      -e "s/\$SECRET/$SECRET/g" \
      -e "s/\$SHARD_ID/$SHARD_ID/g" \
      "$file" > "$CONFIG_DIR$filename";
  fi
done

# Run clickhouse entrypoint
/entrypoint.sh
