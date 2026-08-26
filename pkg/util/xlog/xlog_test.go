package xlog

import (
	"fmt"
	"sync"
	"testing"
)

func TestLoggerPrefixOperationsAreConcurrentSafe(t *testing.T) {
	logger := New()
	const workers = 8
	const iterations = 1_000

	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for iteration := range iterations {
				logger.AddPrefix(LogPrefix{
					Name:     fmt.Sprintf("worker-%d", worker),
					Value:    fmt.Sprintf("%d", iteration),
					Priority: worker + 1,
				})
				_ = logger.Spawn()
				_ = logger.prefix()
			}
		})
	}
	wg.Wait()

	if got := len(logger.ResetPrefixes()); got != workers {
		t.Fatalf("prefix count = %d, want %d", got, workers)
	}
}
