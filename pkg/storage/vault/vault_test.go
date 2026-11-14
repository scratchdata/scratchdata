package vault

import (
	"context"
	"errors"
	"testing"

	aws_sdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/aws"
)

type mockSecretsClient struct {
	store map[string]string
}

func (m *mockSecretsClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	val, ok := m.store[*params.SecretId]
	if !ok {
		return nil, errors.New("ResourceNotFoundException")
	}
	return &secretsmanager.GetSecretValueOutput{
		SecretString: aws_sdk.String(val),
	}, nil
}

func (m *mockSecretsClient) CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	if _, exists := m.store[*params.Name]; exists {
		return nil, &types.ResourceExistsException{Message: aws_sdk.String("already exists")}
	}
	m.store[*params.Name] = *params.SecretString
	return &secretsmanager.CreateSecretOutput{Name: params.Name}, nil
}

func (m *mockSecretsClient) PutSecretValue(ctx context.Context, params *secretsmanager.PutSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error) {
	m.store[*params.SecretId] = *params.SecretString
	return &secretsmanager.PutSecretValueOutput{Name: params.SecretId}, nil
}

func (m *mockSecretsClient) DeleteSecret(ctx context.Context, params *secretsmanager.DeleteSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error) {
	delete(m.store, *params.SecretId)
	return &secretsmanager.DeleteSecretOutput{Name: params.SecretId}, nil
}

func TestMemoryVault(t *testing.T) {
	dests := []config.Destination{
		{
			ID:   1,
			Name: "Local DuckDB file",
			Type: "duckdb",
			Settings: map[string]any{
				"file": "./data/data.duckdb",
			},
		},
	}

	v, err := NewVault(config.Vault{Type: "memory"}, dests)
	if err != nil {
		t.Fatalf("unexpected error creating memory vault: %v", err)
	}

	creds, err := v.GetCredentials("1")
	if err != nil {
		t.Fatalf("expected preloaded credentials for destination 1: %v", err)
	}
	if creds["file"] != "./data/data.duckdb" {
		t.Fatalf("unexpected file setting: %v", creds["file"])
	}

	if err := v.SetCredential("custom_key", `{"host":"localhost"}`); err != nil {
		t.Fatalf("SetCredential failed: %v", err)
	}
	if got := v.GetCredential("custom_key"); got != `{"host":"localhost"}` {
		t.Fatalf("unexpected GetCredential result: %s", got)
	}
	if err := v.DeleteCredential("custom_key"); err != nil {
		t.Fatalf("DeleteCredential failed: %v", err)
	}
	if got := v.GetCredential("custom_key"); got != "" {
		t.Fatalf("expected empty credential after delete, got: %s", got)
	}
}

func TestAWSSecretsManagerVaultWithPrefix(t *testing.T) {
	mock := &mockSecretsClient{store: map[string]string{}}
	v := aws.NewVaultWithClient("scratchdata/prod", mock)

	err := v.SetCredentials("42", map[string]any{"host": "db.internal", "password": "secret"})
	if err != nil {
		t.Fatalf("SetCredentials failed: %v", err)
	}

	if _, ok := mock.store["scratchdata/prod/42"]; !ok {
		t.Fatalf("expected secret to be stored with prefix scratchdata/prod/42")
	}

	err = v.SetCredentials("42", map[string]any{"host": "db2.internal", "password": "updated"})
	if err != nil {
		t.Fatalf("SetCredentials update failed: %v", err)
	}

	creds, err := v.GetCredentials("42")
	if err != nil {
		t.Fatalf("GetCredentials failed: %v", err)
	}
	if creds["password"] != "updated" {
		t.Fatalf("expected updated password, got %v", creds["password"])
	}

	if err := v.Delete("42"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if got := v.GetCredential("42"); got != "" {
		t.Fatalf("expected empty string after deletion, got %s", got)
	}
}
