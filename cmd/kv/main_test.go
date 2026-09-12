package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedDriver(t *testing.T) {
	if os.Getenv("KV_TEST_CLI") == "1" {
		// Arguments after -- belong to the CLI rather than the test runner.
		os.Args = append([]string{os.Args[0]}, flag.Args()...)
		main()
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"unknown_get", []string{"-driver=bogus", "get", "key"}, "unsupported driver \"bogus\"\n"},
		{"unknown_set", []string{"-driver=bogus", "set", "key", "value"}, "unsupported driver \"bogus\"\n"},
		{"empty_get", []string{"-driver=", "get", "key"}, "unsupported driver \"\"\n"},
		{"empty_set", []string{"-driver=", "set", "key", "value"}, "unsupported driver \"\"\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			args := append([]string{"-test.run=^TestUnsupportedDriver$", "--"}, tc.args...)
			cmd := exec.CommandContext(ctx, executable, args...)
			cmd.Env = append(os.Environ(), "KV_TEST_CLI=1")
			cmd.Dir = t.TempDir()
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()
			var exitError *exec.ExitError
			require.ErrorAs(t, err, &exitError)
			assert.Equal(t, 1, exitError.ExitCode())
			assert.Empty(t, stdout.String())
			assert.Equal(t, tc.want, stderr.String())
		})
	}
}

func TestAbort(t *testing.T) {
	// Run abort in a subprocess because it terminates the process with os.Exit.
	if os.Getenv("KV_TEST_ABORT") == "1" {
		abort(os.Getenv("KV_TEST_ABORT_MESSAGE"))
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		message string
	}{
		{"plain", "key not found"},
		{"percent", "100% complete: %s %d %%"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestAbort$")
			cmd.Env = append(os.Environ(), "KV_TEST_ABORT=1", "KV_TEST_ABORT_MESSAGE="+tc.message)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			err := cmd.Run()
			var exitError *exec.ExitError
			require.ErrorAs(t, err, &exitError)
			assert.Equal(t, 1, exitError.ExitCode())
			assert.Empty(t, stdout.String())
			assert.Equal(t, tc.message+"\n", stderr.String())
		})
	}
}
