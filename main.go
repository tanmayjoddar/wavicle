package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"wavicle/internal/config"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
	"wavicle/internal/telemetry"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix("[wavicle] ")

	cfg := config.Default()

	fmt.Println("Wavicle Causal Proof Engine v1.0")
	fmt.Printf("Listening on %s\n", cfg.Server.Listen)

	if err := os.MkdirAll(cfg.Storage.DataDir, 0755); err != nil {
		log.Fatalf("Cannot create data directory: %v", err)
	}

	crystal, err := storage.NewCausalCrystal(filepath.Join(cfg.Storage.DataDir, "crystal.log"))
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	srv := resp3.NewServer(crystal)

	// Start metrics HTTP server
	if cfg.Metrics.Enabled {
		go func() {
			log.Printf("Metrics server on %s/metrics", cfg.Metrics.Listen)
			if err := telemetry.ListenAndServe(cfg.Metrics.Listen); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	} else {
		log.Println("Metrics disabled (enable with metrics.enabled)")
	}

	// Start the RESP3 server in background
	go func() {
		log.Printf("RESP3 server on %s", cfg.Server.Listen)
		if err := srv.ListenAndServe(cfg.Server.Listen); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal %v, shutting down...", sig)

	crystal.Close()
	log.Println("Shutdown complete.")
}