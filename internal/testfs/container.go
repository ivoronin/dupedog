//go:build e2e

package testfs

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// -----------------------------------------------------------------------------
// Container - Generic Docker container wrapper
// -----------------------------------------------------------------------------

// Container wraps a Docker container with a simple exec interface.
type Container struct {
	client      *client.Client
	containerID string
}

// NewContainer creates and starts a Docker container.
//
// The caller is responsible for calling Close() when done.
func NewContainer(ctx context.Context, cfg *container.Config, hostCfg *container.HostConfig) (*Container, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	// Pull image (uses cache if already present)
	if err := pullImage(ctx, cli, cfg.Image); err != nil {
		cli.Close()
		return nil, fmt.Errorf("pull image: %w", err)
	}

	// Create container
	resp, err := cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("create container: %w", err)
	}

	// Start container
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		cli.Close()
		return nil, fmt.Errorf("start container: %w", err)
	}

	return &Container{
		client:      cli,
		containerID: resp.ID,
	}, nil
}

// Run executes a command inside the container.
// Returns stdout, stderr, and exit code.
// If stdin is non-nil, it is written to the command's stdin.
func (c *Container) Run(ctx context.Context, cmd []string, stdin []byte) (stdout, stderr string, exitCode int, err error) {
	execResp, err := c.client.ContainerExecCreate(ctx, c.containerID, container.ExecOptions{
		Cmd:          cmd,
		AttachStdin:  stdin != nil,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", "", 0, fmt.Errorf("exec create: %w", err)
	}

	hijack, err := c.client.ContainerExecAttach(ctx, execResp.ID, container.ExecStartOptions{})
	if err != nil {
		return "", "", 0, fmt.Errorf("exec attach: %w", err)
	}
	defer hijack.Close()

	// Write to stdin if provided
	if stdin != nil {
		if _, err := hijack.Conn.Write(stdin); err != nil {
			return "", "", 0, fmt.Errorf("write stdin: %w", err)
		}
		if err := hijack.CloseWrite(); err != nil {
			return "", "", 0, fmt.Errorf("close stdin: %w", err)
		}
	}

	// Read stdout/stderr
	var outBuf, errBuf bytes.Buffer
	_, _ = stdcopy.StdCopy(&outBuf, &errBuf, hijack.Reader)

	// Get exit code
	inspectResp, err := c.client.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return "", "", 0, fmt.Errorf("exec inspect: %w", err)
	}

	return outBuf.String(), errBuf.String(), inspectResp.ExitCode, nil
}

// Close stops the container and releases resources.
// The container is automatically removed if AutoRemove was set.
func (c *Container) Close(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	defer c.client.Close()
	return c.client.ContainerStop(ctx, c.containerID, container.StopOptions{})
}

// pullImage pulls the Docker image (uses cache if already present).
func pullImage(ctx context.Context, cli *client.Client, imageName string) error {
	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image: %w", err)
	}
	defer reader.Close()
	_, _ = io.Copy(io.Discard, reader)
	return nil
}
