package ci_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestMakeFindsFreshlyInstalledGoTools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(repositoryRoot(t), "hack", "test-tool-path.sh"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("installed tool PATH contract: %v\n%s", err, output)
	}
}
