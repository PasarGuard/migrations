package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	configFile string
	config     *Config
	pathLookup PathLookup
)

func init() {
	flag.StringVar(&configFile, "config", "subscription_url_mapping.json", "Path to the mapping configuration file")
	flag.Parse()
}

func main() {
	log.Println("Starting Subscription URL Redirect Server...")

	// Load configuration
	var err error
	config, err = LoadConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Loaded configuration with %d user mappings", len(config.Mappings))

	// Build path lookup
	pathLookup = BuildPathLookup(config)
	log.Printf("Built path lookup with %d entries", len(pathLookup))

	// Setup HTTP handler
	http.HandleFunc("/", redirectHandler)

	// Prepare server address
	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)

	// Create HTTP server
	server := &http.Server{
		Addr:         addr,
		Handler:      http.DefaultServeMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		if config.Server.SSL.Enabled {
			log.Printf("Starting HTTPS server on %s", addr)

			// Create TLS config from embedded cert and key
			cert, err := tls.X509KeyPair([]byte(config.Server.SSL.Cert), []byte(config.Server.SSL.Key))
			if err != nil {
				log.Fatalf("Failed to load SSL certificate: %v", err)
			}

			server.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}

			if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log.Fatalf("HTTPS server error: %v", err)
			}
		} else {
			log.Printf("Starting HTTP server on %s", addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("HTTP server error: %v", err)
			}
		}
	}()

	log.Println("Server started successfully. Press Ctrl+C to stop.")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down server gracefully...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}

// redirectHandler handles all incoming requests and performs redirects
func redirectHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Look up the new URL in the path lookup
	newURL, found := pathLookup[path]

	if !found {
		// No mapping found, return 404
		log.Printf("404 Not Found: %s", path)
		http.NotFound(w, r)
		return
	}

	// Determine request scheme
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}

	// Get the request host
	host := r.Host

	// Build the final redirect URL
	redirectURL := GetRedirectURL(newURL, config.Server.RedirectDomain, scheme, host)

	// Log the redirect
	log.Printf("Redirecting: %s -> %s", path, redirectURL)

	// Perform 301 Moved Permanently redirect
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
}
