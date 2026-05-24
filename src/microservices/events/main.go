package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieEventsTopic   = "movie-events"
	userEventsTopic    = "user-events"
	paymentEventsTopic = "payment-events"
)

type eventService struct {
	brokers []string
	writers map[string]*kafka.Writer
}

type eventEnvelope struct {
	Type      string          `json:"type"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func main() {
	brokers := parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092"))
	service := newEventService(brokers)
	defer service.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.ensureTopics(ctx); err != nil {
		log.Printf("Kafka topic initialization skipped or failed: %v", err)
	}

	for topic := range service.writers {
		go service.consume(ctx, topic)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", healthHandler)
	mux.HandleFunc("/api/events/movie", service.eventHandler("movie", movieEventsTopic))
	mux.HandleFunc("/api/events/user", service.eventHandler("user", userEventsTopic))
	mux.HandleFunc("/api/events/payment", service.eventHandler("payment", paymentEventsTopic))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Starting events microservice on port %s; kafka brokers: %s", port, strings.Join(brokers, ","))
	log.Fatal(server.ListenAndServe())
}

func newEventService(brokers []string) *eventService {
	return &eventService{
		brokers: brokers,
		writers: map[string]*kafka.Writer{
			movieEventsTopic: {
				Addr:                   kafka.TCP(brokers...),
				Topic:                  movieEventsTopic,
				Balancer:               &kafka.LeastBytes{},
				AllowAutoTopicCreation: true,
			},
			userEventsTopic: {
				Addr:                   kafka.TCP(brokers...),
				Topic:                  userEventsTopic,
				Balancer:               &kafka.LeastBytes{},
				AllowAutoTopicCreation: true,
			},
			paymentEventsTopic: {
				Addr:                   kafka.TCP(brokers...),
				Topic:                  paymentEventsTopic,
				Balancer:               &kafka.LeastBytes{},
				AllowAutoTopicCreation: true,
			},
		},
	}
}

func (s *eventService) eventHandler(eventType, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !json.Valid(payload) {
			http.Error(w, "invalid json payload", http.StatusBadRequest)
			return
		}

		envelope := eventEnvelope{
			Type:      eventType,
			Topic:     topic,
			Payload:   payload,
			CreatedAt: time.Now().UTC(),
		}

		body, err := json.Marshal(envelope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if err := s.writers[topic].WriteMessages(ctx, kafka.Message{
			Key:   []byte(eventType),
			Value: body,
			Time:  envelope.CreatedAt,
		}); err != nil {
			log.Printf("Failed to publish %s event: %v", eventType, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("Published %s event to %s: %s", eventType, topic, string(payload))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "success",
			"topic":  topic,
		})
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (s *eventService) consume(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        s.brokers,
		Topic:          topic,
		GroupID:        "cinemaabyss-events-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Failed to consume from %s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("Consumed event from %s partition=%d offset=%d key=%s value=%s", topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
	}
}

func (s *eventService) ensureTopics(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", s.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(
		kafka.TopicConfig{Topic: movieEventsTopic, NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: userEventsTopic, NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: paymentEventsTopic, NumPartitions: 1, ReplicationFactor: 1},
	)
}

func (s *eventService) close() {
	for _, writer := range s.writers {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
		}
	}
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return []string{"localhost:9092"}
	}
	return brokers
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
