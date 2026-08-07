package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/musix/backhaul/cmd"
	"github.com/musix/backhaul/config"
	"github.com/musix/backhaul/internal/utils"
)

var logger = utils.NewLogger("info")

const (
	version               = "v0.7.2"
	reloadPollInterval    = 2 * time.Second
	reloadShutdownTimeout = 5 * time.Second
)

func main() {
	configPath := flag.String("c", "", "path to the configuration file (TOML format)")
	showVersion := flag.Bool("v", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		logger.Fatalf("Usage: %s -c /path/to/config.toml", flag.CommandLine.Name())
	}

	initialConfig, err := cmd.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("failed to load configuration: %v", err)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := supervise(rootCtx, *configPath, initialConfig); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatalf("runtime stopped: %v", err)
	}
}

// supervise owns configuration generations. A replacement is parsed and
// validated before the running generation is canceled, so a partial/invalid
// file write cannot take a healthy tunnel down during hot reload.
func supervise(rootCtx context.Context, configPath string, initialConfig *config.Config) error {
	lastModTime, err := getLastModTime(configPath)
	if err != nil {
		return fmt.Errorf("get config modification time: %w", err)
	}

	runCtx, cancelRun := context.WithCancel(rootCtx)
	runDone := startGeneration(runCtx, initialConfig)

	ticker := time.NewTicker(reloadPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rootCtx.Done():
			cancelRun()
			return waitGeneration(runDone, reloadShutdownTimeout)

		case err := <-runDone:
			cancelRun()
			if err != nil {
				return err
			}
			if rootCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("configuration generation stopped unexpectedly")

		case <-ticker.C:
			modTime, err := getLastModTime(configPath)
			if err != nil {
				logger.Errorf("error checking config modification time: %v", err)
				continue
			}
			if modTime.Equal(lastModTime) {
				continue
			}
			lastModTime = modTime

			nextConfig, err := cmd.LoadConfig(configPath)
			if err != nil {
				logger.Errorf("config changed but validation failed; keeping current generation: %v", err)
				continue
			}

			logger.Info("config file changed; starting a validated replacement generation")
			cancelRun()
			if err := waitGeneration(runDone, reloadShutdownTimeout); err != nil {
				return fmt.Errorf("stop previous configuration generation: %w", err)
			}
			if rootCtx.Err() != nil {
				return nil
			}

			runCtx, cancelRun = context.WithCancel(rootCtx)
			runDone = startGeneration(runCtx, nextConfig)
		}
	}
}

func startGeneration(ctx context.Context, cfg *config.Config) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- cmd.RunConfig(cfg, ctx)
	}()
	return done
}

func waitGeneration(done <-chan error, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("shutdown exceeded %s", timeout)
	}
}

func getLastModTime(file string) (time.Time, error) {
	absPath, err := filepath.Abs(file)
	if err != nil {
		return time.Time{}, err
	}
	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return time.Time{}, err
	}
	return fileInfo.ModTime(), nil
}
