package transport

import "scratchdata/pkg/accounts"

type Transport interface {
	GetAccountManager() accounts.AccountManagement

	StartProducer() error
	StopProducer() error

	Write(databaseConnectionId string, data []byte) error

	StartConsumer() error
	StopConsumer() error
}
