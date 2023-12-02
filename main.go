package main

import (
	"context"
	"os"
	"os/signal"
	"scratchdata/cmd"
	"scratchdata/cmd/api"
	"scratchdata/pkg/accounts"
	"scratchdata/pkg/accounts/dummy"
	"scratchdata/pkg/queue"
	"scratchdata/pkg/storage"
	"scratchdata/pkg/transport"
	"scratchdata/pkg/transport/queuestorage"
	"strconv"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func setupLogs() {
	// Equivalent of Lshortfile
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	// log.Logger = log.With().Caller().Logger()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
}

func main() {
	setupLogs()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var accountManager accounts.AccountManagement
	accountManager = dummy.DummyAccountManager{}

	var queueBackend queue.QueueBackend
	var storageBackend storage.StorageBackend

	var dataTransport transport.DataTransport
	dataTransport = queuestorage.NewQueueStorageTransport(queueBackend, storageBackend)

	var command cmd.Command
	command = api.NewAPIServer(accountManager, dataTransport)
	command.Start()

	select {
	case <-ctx.Done():
		command.Stop()
	}
}
