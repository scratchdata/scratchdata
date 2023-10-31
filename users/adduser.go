package users

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"scratchdb/servers"
	"text/template"

	"github.com/cqroot/prompt"
	"github.com/cqroot/prompt/multichoose"
)

// Replica represents each replica item within a shard
type Replica struct {
	Priority int
	Host     string
	Port     int
}

// Shard represents each shard item which contains replicas
type Shard struct {
	Weight              int
	InternalReplication bool
	Replicas            []Replica
}

// Server represents the overall structure for the XML template
type Server struct {
	Name   string
	Secret string
	Shards []Shard
}

var clusterTemplate string = `
<clickhouse>
	<remote_servers>
		<{{.Name}}>
			<secret>{{.Secret}}</secret>
			{{range .Shards}}
			<shard>
				<weight>{{.Weight}}</weight>
				<internal_replication>{{.InternalReplication}}</internal_replication>
				{{range .Replicas}}
				<replica>
					<priority>{{.Priority}}</priority>
					<host>{{.Host}}</host>
					<port>{{.Port}}</port>
				</replica>
				{{end}}
			</shard>
			{{end}}
		</{{.Name}}>
	</remote_servers>
</clickhouse>
`

type DefaultUserManager struct {
}

func filter(A, B []string) []string {
	bMap := make(map[string]bool)
	for _, b := range B {
		bMap[b] = true
	}

	result := []string{}
	for _, a := range A {
		if _, found := bMap[a]; !found {
			result = append(result, a)
		}
	}

	return result
}

func (m *DefaultUserManager) GetDBManager() servers.ClickhouseManager {
	return &servers.DefaultServerManager{}
}

func (m *DefaultUserManager) generateServerConfig(name string, secret string, shards []string, replicas []string) Server {
	rc := Server{
		Name:   name,
		Secret: secret,
	}

	replicasPerShard := len(replicas) / len(shards)
	replicaIndex := 0
	for _, shard := range shards {
		s := Shard{}
		r := Replica{
			Host: shard,
		}
		s.Replicas = append(s.Replicas, r)

		for j := 0; j < replicasPerShard; j++ {
			r := Replica{
				Host: replicas[replicaIndex],
			}
			s.Replicas = append(s.Replicas, r)
			replicaIndex++
		}

		rc.Shards = append(rc.Shards, s)
	}

	return rc
}

func (m *DefaultUserManager) generateClickhouseXML(data Server) string {
	tmpl, err := template.New("xmlTemplate").Parse(clusterTemplate)
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

// TODO: this should take a list of servers as input since we'll do this from the UI in the future
func (m *DefaultUserManager) AddUser(name string) error {
	if name == "" {
		return errors.New("Invalid user")
	}

	// TODO: check to see if user already exists

	log.Println("Adding user", name)

	serverHosts := []string{}
	for _, host := range m.GetDBManager().GetServers() {
		serverHosts = append(serverHosts, host.GetHost())
	}

	shards, _ := prompt.New().Ask("Shards:").
		MultiChoose(
			serverHosts,
			multichoose.WithHelp(true),
		)

	hostsForReplicas := filter(serverHosts, shards)
	replicas, _ := prompt.New().Ask("Replicas:").
		MultiChoose(
			hostsForReplicas,
			multichoose.WithHelp(true),
		)

	// TODO: let the user be able to have some shards with replicas and
	// others without if they really want to
	if len(replicas)%len(shards) != 0 {
		log.Println("each shard must have the same number of replicas")
		os.Exit(1)
	}

	clusterSecret := "secret"
	data := m.generateServerConfig(name, clusterSecret, shards, replicas)

	fmt.Println(m.generateClickhouseXML(data))

	return nil
}
