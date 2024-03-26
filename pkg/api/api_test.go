package api

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"
)

func TestAPI(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fmt.Println("Error generating key:", err)
		return
	}

	derPrivateKey := x509.MarshalPKCS1PrivateKey(privateKey)

	pemBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: derPrivateKey,
	}

	pemPrivateKey := pem.EncodeToMemory(pemBlock)

	fmt.Println(string(pemPrivateKey))
}
