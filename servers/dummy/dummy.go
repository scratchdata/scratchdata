package dummy

import (
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
)

type DummyDBServer struct {
}

func NewDummyDBServer() *DummyDBServer {
	return &DummyDBServer{}
}

func (s *DummyDBServer) InsertBatchFromNDJson(input io.Reader) error {
	data, err := io.ReadAll(input)
	if err != nil {
		return err
	}

	log.Debug().Bytes("data", data).Msg("Writing Data to DB")
	return nil
}

func (s *DummyDBServer) QueryJSON(query string, writer io.Writer) error {
	log.Debug().Str("query", query).Msg("Querying")
	// reader, writer := io.Pipe()
	// go func() {
	for i := 0; i < 10; i++ {
		data := "Data " + strconv.Itoa(i) + "\n"
		_, err := writer.Write([]byte(data))
		if err != nil {
			fmt.Println("Error writing data:", err)
			break
		}
	}
	// }()
	// Run the producer in a goroutine, as it'll block waiting for the reader

	// go s.produce(writer)

	return nil

	// Read from the reader in the main goroutine
	// buf := make([]byte, 32) // A small buffer for demonstration purposes
	// for {
	// 	n, err := reader.Read(buf)
	// 	if err == io.EOF { // io.EOF indicates end of stream
	// 		break
	// 	}
	// 	if err != nil {
	// 		fmt.Println("Error reading data:", err)
	// 		break
	// 	}
	// 	fmt.Print(string(buf[:n]))
	// }
}
