package ssh

import (
	"errors"
	"fmt"
	"io"
	"scratchdata/util"

	gossh "github.com/helloyi/go-sshclient"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

type SSHServer struct {
	PrivateKey string `mapstructure:"private_key" description:"SSH Private Key"`
	User       string `mapstructure:"user" description:"Username"`
	Host       string `mapstructure:"host" description:"Hostname"`
	Port       string `mapstructure:"port" description:"Port"`
	Directory  string `mapstructure:"directory" description:"Full path, including file name, to store data"`
}

func (s *SSHServer) InsertBatchFromNDJson(input io.ReadSeeker) error {
	return errors.New("Not implemented for ssh")
}

func (s *SSHServer) openSSHConnection() (*gossh.Client, error) {
	signer, err := ssh.ParsePrivateKey([]byte(s.PrivateKey))
	if err != nil {
		return nil, err
	}

	// Create SSH client config
	sshConfig := &ssh.ClientConfig{
		User: s.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Note: Avoid using InsecureIgnoreHostKey in production
	}

	// Connect to SSH
	address := fmt.Sprintf("%s:%s", s.Host, s.Port)
	sshClient, err := gossh.Dial("tcp", address, sshConfig)
	return sshClient, err
	// sshClient, err := ssh.Dial("tcp", address, sshConfig)
	// return sshClient, err
}

func (s *SSHServer) setupDuckDB(sshClient *gossh.Client) {
	rc := sshClient.Cmd("mkdir -p " + s.Directory)
	b, e := rc.Output()
	log.Print(e)
	log.Print(string(b))

	rc = sshClient.Cmd("./duckdb")
	b, e = rc.Output()
	log.Print(e)
	log.Print(string(b))

	if e != nil {
		sftp := sshClient.Sftp()
		err := sftp.Upload("pkg/destinations/ssh/duckdb", "duckdb")
		log.Print(err)
		e = sshClient.Cmd("chmod 755 duckdb").Run()
		log.Print(e)
		// b, e = sshClient.Cmd("wget https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-amd64.zip").Cmd("unzip duckdb_cli-linux-amd64.zip").Output()
		// log.Println(e)
		// log.Println(string(b))
	}
	// Start a session
	// session, err := sshClient.NewSession()
	// if err != nil {
	// 	log.Error().Err(err).Msg("Failed to create sessions")
	// 	return
	// }

	// data, err := session.StdoutPipe()
	// log.Print(err)
	// // log.Print(io.ReadAll(data))
	// log.Print(session.Run("ls"))
	// log.Print(session.Run("ls -a"))
	// b, err := io.ReadAll(data)
	// log.Print(err)
	// log.Print(string(b))
	// session.
	// 	session.Close()
}

func (s *SSHServer) queryDuckDB(sshClient *gossh.Client, writer io.Writer, query string) error {
	// Get tables available on remote server
	tables := []string{}
	files, err := sshClient.Sftp().ReadDir(s.Directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			tables = append(tables, file.Name())
		}
	}

	// Prepare SQL query
	sql := "INSTALL 'parquet'; LOAD 'parquet'; "

	for i, table := range tables {
		if i > 0 {
			sql += ","
		}
		sql += fmt.Sprintf("\nWITH \"%s\" as (select * from read_parquet('%s/%s/*.parquet', filename=true,file_row_number=true,union_by_name=true)) ", table, s.Directory, table)
	}

	sql += " SELECT * FROM (" + query + ") "

	log.Trace().Str("sql", sql).Send()

	// Perform query, return JSON results
	c := sshClient.UnderlyingClient()
	se, err := c.NewSession()
	if err != nil {
		return err
	}
	defer se.Close()
	p, err := se.StdinPipe()
	if err != nil {
		return err
	}

	se.Stdout = writer
	se.Stderr = writer

	err = se.Start("./duckdb --json")
	if err != nil {
		return err
	}

	_, err = io.WriteString(p, sql)
	if err != nil {
		return err
	}

	err = p.Close()
	if err != nil {
		return err
	}

	err = se.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (s *SSHServer) QueryJSON(query string, writer io.Writer) error {
	sshClient, err := s.openSSHConnection()
	if err != nil {
		return err
	}

	s.setupDuckDB(sshClient)

	sanitized := util.TrimQuery(query)
	err = s.queryDuckDB(sshClient, writer, sanitized)
	return err
}
