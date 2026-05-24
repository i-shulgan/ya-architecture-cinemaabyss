package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type proxyConfig struct {
	monolithURL            *url.URL
	moviesServiceURL       *url.URL
	eventsServiceURL       *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
	monolithProxy          *httputil.ReverseProxy
	moviesServiceProxy     *httputil.ReverseProxy
	eventsServiceProxy     *httputil.ReverseProxy
}

func main() {
	config := loadConfig()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", config.proxyHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Starting Strangler Fig proxy on port %s", port)
	log.Fatal(server.ListenAndServe())
}

func loadConfig() *proxyConfig {
	monolithURL := mustParseURL(getEnv("MONOLITH_URL", "http://localhost:8080"))
	moviesServiceURL := mustParseURL(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"))
	eventsServiceURL := mustParseURL(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"))

	percent, err := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT, using 0: %v", err)
		percent = 0
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	return &proxyConfig{
		monolithURL:            monolithURL,
		moviesServiceURL:       moviesServiceURL,
		eventsServiceURL:       eventsServiceURL,
		gradualMigration:       strings.EqualFold(getEnv("GRADUAL_MIGRATION", "false"), "true"),
		moviesMigrationPercent: percent,
		monolithProxy:          newReverseProxy(monolithURL),
		moviesServiceProxy:     newReverseProxy(moviesServiceURL),
		eventsServiceProxy:     newReverseProxy(eventsServiceURL),
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (c *proxyConfig) proxyHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if c.routeMoviesToService() {
			log.Printf("Proxying %s %s to movies-service", r.Method, r.URL.RequestURI())
			c.moviesServiceProxy.ServeHTTP(w, r)
			return
		}
		log.Printf("Proxying %s %s to monolith", r.Method, r.URL.RequestURI())
		c.monolithProxy.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		log.Printf("Proxying %s %s to events-service", r.Method, r.URL.RequestURI())
		c.eventsServiceProxy.ServeHTTP(w, r)
	default:
		log.Printf("Proxying %s %s to monolith", r.Method, r.URL.RequestURI())
		c.monolithProxy.ServeHTTP(w, r)
	}
}

func (c *proxyConfig) routeMoviesToService() bool {
	if !c.gradualMigration {
		return false
	}
	if c.moviesMigrationPercent <= 0 {
		return false
	}
	if c.moviesMigrationPercent >= 100 {
		return true
	}

	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		log.Printf("Cannot read random bytes, falling back to monolith: %v", err)
		return false
	}
	return int(binary.BigEndian.Uint64(bytes[:])%100) < c.moviesMigrationPercent
}

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Proxy-Service", "cinemaabyss-proxy")
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error for %s %s: %v", r.Method, r.URL.RequestURI(), err)
		http.Error(w, "upstream service unavailable", http.StatusBadGateway)
	}
	return proxy
}

func mustParseURL(rawURL string) *url.URL {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Invalid upstream URL %q: %v", rawURL, err)
	}
	return parsedURL
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
