package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// SSLConfig holds SSL certificate configuration
type SSLConfig struct {
	Enabled bool   `json:"enabled"`
	Cert    string `json:"cert"`
	Key     string `json:"key"`
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Host           string    `json:"host"`
	Port           int       `json:"port"`
	RedirectDomain string    `json:"redirect_domain"`
	SSL            SSLConfig `json:"ssl"`
}

// UserMapping holds the mapping for a single user
type UserMapping struct {
	UserID              int    `json:"user_id"`
	OldSubscriptionURL  string `json:"old_subscription_url"`
	NewSubscriptionURL  string `json:"new_subscription_url"`
	UsernamePasarguard  string `json:"username_pasarguard,omitempty"`
	MatchedBy           string `json:"matched_by,omitempty"`
}

// MappingData holds the URL mapping data
type MappingData struct {
	GeneratedAt   string                 `json:"generated_at"`
	TotalUsers    int                    `json:"total_users"`
	MappedUsers   int                    `json:"mapped_users"`
	NotFoundUsers int                    `json:"not_found_users"`
	URLFormats    map[string]string      `json:"url_formats"`
	Mappings      map[string]UserMapping `json:"mappings"`
	NotFound      map[string]UserMapping `json:"not_found,omitempty"`
}

// PathLookup is a reverse lookup map from old path to new URL
type PathLookup map[string]string

// LoadServerConfig loads the server configuration from a JSON file
func LoadServerConfig(filename string) (*ServerConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate server config
	if config.Port <= 0 || config.Port > 65535 {
		return nil, fmt.Errorf("invalid port number: %d", config.Port)
	}

	if config.SSL.Enabled {
		if config.SSL.Cert == "" || config.SSL.Key == "" {
			return nil, fmt.Errorf("SSL enabled but cert or key is empty")
		}
	}

	return &config, nil
}

// LoadMappingData loads the URL mapping data from a JSON file
func LoadMappingData(filename string) (*MappingData, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping file: %w", err)
	}

	var mappingData MappingData
	if err := json.Unmarshal(data, &mappingData); err != nil {
		return nil, fmt.Errorf("failed to parse mapping file: %w", err)
	}

	return &mappingData, nil
}

// BuildPathLookup creates a reverse lookup map from old paths to new URLs
func BuildPathLookup(mappingData *MappingData) PathLookup {
	lookup := make(PathLookup)

	for _, mapping := range mappingData.Mappings {
		// Extract path from old URL (remove protocol and domain if present)
		oldPath := extractPath(mapping.OldSubscriptionURL)

		// Store the mapping
		lookup[oldPath] = mapping.NewSubscriptionURL
	}

	return lookup
}

// extractPath extracts the path portion from a URL
// Examples:
//   - "/sub/user/key" -> "/sub/user/key"
//   - "https://example.com/sub/user/key" -> "/sub/user/key"
func extractPath(url string) string {
	// If URL starts with http:// or https://, extract path
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		// Find the third slash (after protocol)
		slashCount := 0
		for i, char := range url {
			if char == '/' {
				slashCount++
				if slashCount == 3 {
					return url[i:]
				}
			}
		}
		// If no path found, return "/"
		return "/"
	}

	// Already a path
	return url
}

// GetRedirectURL constructs the final redirect URL
func GetRedirectURL(newURL, redirectDomain, requestScheme, requestHost string) string {
	// If newURL is absolute (has protocol), use it as-is
	if strings.HasPrefix(newURL, "http://") || strings.HasPrefix(newURL, "https://") {
		return newURL
	}

	// If redirect_domain is specified, use it
	if redirectDomain != "" {
		// Ensure redirect_domain has protocol
		if !strings.HasPrefix(redirectDomain, "http://") && !strings.HasPrefix(redirectDomain, "https://") {
			redirectDomain = "https://" + redirectDomain
		}
		return strings.TrimSuffix(redirectDomain, "/") + newURL
	}

	// Otherwise, use the request's scheme and host
	return requestScheme + "://" + requestHost + newURL
}
