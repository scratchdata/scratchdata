package util

import (
	"crypto/sha256"
	"github.com/bwmarrin/snowflake"
	"os"
)

func NewSnowflakeGenerator() (*snowflake.Node, error) {
	// Get the current hostname
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// Hash the hostname using SHA-256
	hash := sha256.Sum256([]byte(hostname))

	// Convert the last byte of the hash to uint32, but we only need the lower 10 bits
	// Note: The hash is a byte array, and we are only working with the last byte for simplicity
	lastByte := hash[len(hash)-1]          // Get the last byte of the hash
	lower10Bits := int64(lastByte) & 0x3FF // Mask to get lower 10 bits

	node, err := snowflake.NewNode(lower10Bits)
	if err != nil {
		return nil, err
	}
	return node, nil
}
