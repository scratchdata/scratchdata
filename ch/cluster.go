package ch

import (
	"context"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Replica struct {
	Name                  string        `json:"name"`
	IsActive              bool          `json:"is_active"`
	EstimatedRecoveryTime time.Duration `json:"estimated_recovery_time"`
	ReplicaNum            uint32        `json:"replica_num"`
	HostName              string        `json:"host_name"`
	HostAddress           string        `json:"host_address"`
	Port                  uint32        `json:"port"`
	IsLocal               bool          `json:"is_local"`
	User                  string        `json:"user"`
	DefaultDatabase       string        `json:"default_database"`
}

type Shard struct {
	Name        string    `json:"name"`
	ShardWeight uint32    `json:"shard_weight"`
	ShardNum    uint32    `json:"shard_num"`
	Replicas    []Replica `json:"replicas"`
}

type Cluster struct {
	Name   string  `json:"name"`
	Shards []Shard `json:"shards"`
}

func getClusterDetails(ctx context.Context, conn clickhouse.Conn, name string) (Cluster, error) {
	type ClusterDetail struct {
		Cluster               string `json:"cluster"`
		ShardNum              uint32 `json:"shard_num"`
		ShardWeight           uint32 `json:"shard_weight"`
		ReplicaNum            uint32 `json:"replica_num"`
		HostName              string `json:"host_name"`
		HostAddress           string `json:"host_address"`
		Port                  uint32 `json:"port"`
		IsLocal               uint32 `json:"is_local"`
		User                  string `json:"user"`
		DefaultDatabase       string `json:"default_database"`
		ErrorsCount           uint32 `json:"errors_count"`
		SlowdownsCount        uint32 `json:"slowdowns_count"`
		EstimatedRecoveryTime uint32 `json:"estimated_recovery_time"`
		DatabaseShardName     string `json:"database_shard_name"`
		DatabaseReplicaName   string `json:"database_replica_name"`
		IsActive              uint32 `json:"is_active"`
	}

	query := "SELECT * FROM system.clusters WHERE cluster = ?"
	rows, err := conn.Query(ctx, query, name)
	if err != nil {
		return Cluster{}, nil
	}
	var replicas []ClusterDetail
	err = rows.ScanStruct(&replicas)
	if err != nil {
		return Cluster{}, err
	}

	hm := map[string]*Shard{}
	for _, replica := range replicas {
		shardName := replica.DatabaseShardName
		if shardName == "" {
			shardName = strconv.Itoa(int(replica.ShardNum))
		}
		shard, ok := hm[shardName]
		if !ok {
			shard = &Shard{
				Name:        replica.DatabaseShardName,
				ShardWeight: replica.ShardWeight,
				ShardNum:    replica.ShardNum,
				Replicas:    make([]Replica, 0),
			}
		}
		shard.Replicas = append(shard.Replicas, Replica{
			Name:                  replica.DatabaseReplicaName,
			IsActive:              replica.IsActive == 1,
			EstimatedRecoveryTime: time.Duration(replica.EstimatedRecoveryTime),
			ReplicaNum:            replica.ReplicaNum,
			HostName:              replica.HostName,
			HostAddress:           replica.HostAddress,
			Port:                  replica.Port,
			IsLocal:               replica.IsLocal == 1,
			User:                  replica.User,
			DefaultDatabase:       replica.DefaultDatabase,
		})
		hm[shardName] = shard
	}

	var shards []Shard
	for _, shard := range hm {
		shards = append(shards, *shard)
	}

	return Cluster{
		Name:   name,
		Shards: shards,
	}, nil
}
