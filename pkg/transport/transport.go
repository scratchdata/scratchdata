package transport

type DataTransport interface {
	StartProducer() error
	StopProducer() error

	Write(databaseConnectionId string, data []byte) error

	StartConsumer() error
	StopConsumer() error
}
