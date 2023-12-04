package transport

import "scratchdata/pkg/accounts"

type DataTransport interface {
	GetAccountManager() accounts.AccountManager

	StartProducer() error
	StopProducer() error

	Write(databaseConnectionId string, data []byte) error

	StartConsumer() error
	StopConsumer() error
}
