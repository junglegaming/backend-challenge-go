package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	localStackImage = "localstack/localstack:3.8"
	localStackPort  = "4566/tcp"
)

// LocalStack is a running AWS emulator with SQS enabled.
type LocalStack struct {
	// Endpoint is the base URL AWS SDK clients should be pointed at.
	Endpoint  string
	container testcontainers.Container
}

// StartLocalStack runs LocalStack with only SQS enabled. Waiting for "Ready."
// means the emulator accepts requests, not merely that its port is open.
func StartLocalStack(ctx context.Context) (*LocalStack, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        localStackImage,
			ExposedPorts: []string{localStackPort},
			Env:          map[string]string{"SERVICES": "sqs"},
			WaitingFor:   wait.ForLog("Ready.").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start localstack: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("localstack host: %w", err)
	}
	port, err := container.MappedPort(ctx, localStackPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("localstack port: %w", err)
	}

	return &LocalStack{
		Endpoint:  fmt.Sprintf("http://%s:%s", host, port.Port()),
		container: container,
	}, nil
}

// Terminate stops and removes the container.
func (l *LocalStack) Terminate(ctx context.Context) error {
	return l.container.Terminate(ctx)
}
