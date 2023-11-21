#!/bin/bash
set -e

declare -A users=(
    [userOne]="clusterA"
    [userTwo]="clusterB"
)

run_as_file() {
  sql=""
  for user in "${!users[@]}"; do
    cluster="${users[$user]}"
    sql+="CREATE DATABASE IF NOT EXISTS ${user} ON CLUSTER ${cluster};"
    sql+="CREATE USER IF NOT EXISTS ${user} ON CLUSTER ${cluster} IDENTIFIED BY '${user}' DEFAULT DATABASE ${user};"
    sql+="GRANT ON CLUSTER ${cluster} ALL ON ${user}.* TO ${user} WITH GRANT OPTION;"
  done

  sql+="SHOW DATABASES;"

#  printf "===DEBUG===\n===\n%s\n===\n" "$sql"

  clickhouse client --multiquery "$sql"
#  clickhouse client --queries-file <(echo "${sql}")

}

run() {
  for user in "${!users[@]}"; do
    cluster="${users[$user]}"

    clickhouse client --query "CREATE DATABASE IF NOT EXISTS ${user} ON CLUSTER ${cluster};"
    clickhouse client --query "CREATE USER IF NOT EXISTS ${user} ON CLUSTER ${cluster} IDENTIFIED BY '${user}' DEFAULT DATABASE ${user};"
    clickhouse client --query "GRANT ON CLUSTER ${cluster} ALL ON ${user}.* TO ${user} WITH GRANT OPTION;"
  done

  clickhouse client --query "SHOW DATABASES;"
}

#run
run_as_file || true
cat /var/log/clickhouse-server/clickhouse-server.err.log && exit 1



# This script currently fails with the following error:
#/entrypoint.sh: running /docker-entrypoint-initdb.d/init-db.sh
#2023-11-21T20:35:04.198387848Z CREATE DATABASE IF NOT EXISTS userTwo ON CLUSTER clusterB;\nCREATE USER IF NOT EXISTS userTwo ON CLUSTER clusterB IDENTIFIED BY 'userTwo' DEFAULT DATABASE userTwo;\nGRANT ON CLUSTER clusterB ALL ON userTwo.* TO userTwo WITH REPLACE OPTION;\nCREATE DATABASE IF NOT EXISTS userOne ON CLUSTER clusterA;\nCREATE USER IF NOT EXISTS userOne ON CLUSTER clusterA IDENTIFIED BY 'userOne' DEFAULT DATABASE userOne;\nGRANT ON CLUSTER clusterA ALL ON userOne.* TO userOne WITH REPLACE OPTION;\nSHOW DATABASES;
#2023-11-21T20:35:04.219553289Z Received exception from server (version 23.10.4):
#2023-11-21T20:35:04.219567139Z Code: 999. DB::Exception: Received from localhost:9000. Coordination::Exception. Coordination::Exception: All connection tries failed while connecting to ZooKeeper. nodes: 192.168.160.2:9181, 192.168.160.3:9181
#2023-11-21T20:35:04.219569751Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.2:9181
#2023-11-21T20:35:04.219571655Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.3:9181
#2023-11-21T20:35:04.219581760Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.2:9181
#2023-11-21T20:35:04.219584087Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.3:9181
#2023-11-21T20:35:04.219585920Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.2:9181
#2023-11-21T20:35:04.219587818Z Poco::Exception. Code: 1000, e.code() = 111, Connection refused (version 23.10.4.25 (official build)), 192.168.160.3:9181
#2023-11-21T20:35:04.219589606Z . (KEEPER_EXCEPTION)
#2023-11-21T20:35:04.219591330Z (query: CREATE DATABASE IF NOT EXISTS userTwo ON CLUSTER clusterB;)
