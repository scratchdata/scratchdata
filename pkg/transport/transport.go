package transport

type DataTransport interface {
	StartProducer() error
	StopProducer() error

	Write(databaseConnectionId string, table string, data []byte) error

	StartConsumer() error
	StopConsumer() error
}
