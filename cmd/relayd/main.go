package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xmrtdao/relay-go/internal/config"
	"github.com/xmrtdao/relay-go/internal/server"
)

func main() {
	cfgPath := flag.String("config", "", "path to config file")
	port := flag.Int("port", 0, "override port (env: RELAY_PORT)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if *port > 0 {
		cfg.Port = *port
	}

	log.Printf("=== Go Relay Agent Daemon v0.1.0 ===")
	log.Printf("Listening on %s:%d", cfg.Host, cfg.Port)
	log.Printf("Log level: %s", cfg.LogLevel)

	srv := server.New(cfg)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal %v — shutting down", sig)
		srv.Stop()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
