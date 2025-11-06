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
	configFile  string
	mappingFile string
	serverCfg   *ServerConfig
	pathLookup  PathLookup
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "Path to the server configuration file")
	flag.StringVar(&configFile, "config", "config.json", "Path to the server configuration file")
	flag.StringVar(&mappingFile, "m", "subscription_url_mapping.json", "Path to the URL mapping file")
	flag.StringVar(&mappingFile, "map", "subscription_url_mapping.json", "Path to the URL mapping file")
	flag.Parse()
}

func main() {
	log.Println("Starting Subscription URL Redirect Server...")

	// Load server configuration
	var err error
	serverCfg, err = LoadServerConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load server config: %v", err)
	}

	log.Printf("Loaded server configuration from %s", configFile)

	// Load mapping data
	mappingData, err := LoadMappingData(mappingFile)
	if err != nil {
		log.Fatalf("Failed to load mapping data: %v", err)
	}

	log.Printf("Loaded %d user mappings from %s", len(mappingData.Mappings), mappingFile)

	// Build path lookup
	pathLookup = BuildPathLookup(mappingData)
	log.Printf("Built path lookup with %d entries", len(pathLookup))

	// Setup HTTP handler
	http.HandleFunc("/", redirectHandler)

	// Prepare server address
	addr := fmt.Sprintf("%s:%d", serverCfg.Host, serverCfg.Port)

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
		if serverCfg.SSL.Enabled {
			log.Printf("Starting HTTPS server on %s", addr)

			// Create TLS config from embedded cert and key
			cert, err := tls.X509KeyPair([]byte(serverCfg.SSL.Cert), []byte(serverCfg.SSL.Key))
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
	redirectURL := GetRedirectURL(newURL, serverCfg.RedirectDomain, scheme, host)

	// Log the redirect
	log.Printf("Redirecting: %s -> %s", path, redirectURL)

	// Perform 301 Moved Permanently redirect
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
}
