package sseredis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	sse "github.com/oarkflow/sse"
)

type RedisReplayStore struct {
	client *redis.Client
	prefix string
	max    int
	ttl    time.Duration
}

type RedisStoreOptions struct {
	Prefix    string
	MaxEvents int
	TTL       time.Duration
}

func NewRedisReplayStore(client *redis.Client, opts RedisStoreOptions) *RedisReplayStore {
	if opts.Prefix == "" {
		opts.Prefix = "sse:replay:"
	}
	if opts.MaxEvents <= 0 {
		opts.MaxEvents = 10000
	}
	if opts.TTL <= 0 {
		opts.TTL = 24 * time.Hour
	}
	return &RedisReplayStore{
		client: client,
		prefix: opts.Prefix,
		max:    opts.MaxEvents,
		ttl:    opts.TTL,
	}
}

type sseEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Data      string `json:"data"`
	Topic     string `json:"topic,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

func (s *RedisReplayStore) Append(event *sse.Event) {
	if event == nil || event.ID == "" {
		return
	}

	ctx := context.Background()
	data, err := json.Marshal(sseEvent{
		ID:        event.ID,
		Type:      event.Type,
		Data:      event.Data,
		Topic:     event.Topic,
		CreatedAt: event.CreatedAt.UnixNano(),
	})
	if err != nil {
		return
	}

	key := s.prefix + "events"

	pipe := s.client.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.LTrim(ctx, key, int64(-s.max), -1)
	pipe.Expire(ctx, key, s.ttl)
	_, _ = pipe.Exec(ctx)
}

func (s *RedisReplayStore) ReplayAfter(lastEventID string, topics map[string]bool, limit int) []*sse.Event {
	ctx := context.Background()
	key := s.prefix + "events"

	if limit <= 0 {
		limit = 500
	}

	results, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil
	}

	var events []*sse.Event
	found := lastEventID == ""

	for _, data := range results {
		var re sseEvent
		if err := json.Unmarshal([]byte(data), &re); err != nil {
			continue
		}

		if !found {
			if re.ID == lastEventID {
				found = true
			}
			continue
		}

		if len(topics) > 0 && re.Topic != "" && !topics[re.Topic] {
			continue
		}

		events = append(events, &sse.Event{
			ID:        re.ID,
			Type:      re.Type,
			Data:      re.Data,
			Topic:     re.Topic,
			CreatedAt: time.Unix(0, re.CreatedAt),
		})

		if len(events) >= limit {
			break
		}
	}

	return events
}

func (s *RedisReplayStore) Clear() error {
	ctx := context.Background()
	return s.client.Del(ctx, s.prefix+"events").Err()
}

func (s *RedisReplayStore) Close() error {
	return s.client.Close()
}
