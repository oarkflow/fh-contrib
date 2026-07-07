package sseredis

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	sse "github.com/oarkflow/sse"
)

type RedisBus struct {
	client  *redis.Client
	channel string
	logger  *slog.Logger
}

type RedisBusOptions struct {
	Channel string
	Logger  *slog.Logger
}

type redisBusMessage struct {
	Target     sse.PublishTarget `json:"target"`
	ClientID   string            `json:"client_id,omitempty"`
	Group      string            `json:"group,omitempty"`
	ExcludeIDs []string          `json:"exclude_ids,omitempty"`
	Event      sseEvent          `json:"event"`
}

func NewRedisBus(client *redis.Client, opts RedisBusOptions) *RedisBus {
	if opts.Channel == "" {
		opts.Channel = "sse:bus"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &RedisBus{
		client:  client,
		channel: opts.Channel,
		logger:  opts.Logger,
	}
}

func (b *RedisBus) Publish(ctx context.Context, publishCtx sse.PublishContext, event *sse.Event) error {
	msg := redisBusMessage{
		Target:     publishCtx.Target,
		ClientID:   publishCtx.ClientID,
		Group:      publishCtx.Group,
		ExcludeIDs: append([]string(nil), publishCtx.ExcludeIDs...),
		Event: sseEvent{
			ID:        event.ID,
			Type:      event.Type,
			Data:      event.Data,
			Topic:     event.Topic,
			CreatedAt: event.CreatedAt.UnixNano(),
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, b.channel, data).Err()
}

func (b *RedisBus) Subscribe(ctx context.Context, hub *sse.Hub) error {
	sub := b.client.Subscribe(ctx, b.channel)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var m redisBusMessage
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				b.logger.Warn("sse redis bus: invalid payload", "error", err.Error())
				continue
			}
			event := &sse.Event{
				ID:        m.Event.ID,
				Type:      m.Event.Type,
				Data:      m.Event.Data,
				Topic:     m.Event.Topic,
				CreatedAt: unixNanoToTime(m.Event.CreatedAt),
			}
			switch m.Target {
			case sse.PublishTargetBroadcast:
				hub.Broadcast(event)
			case sse.PublishTargetClient:
				_ = hub.Send(m.ClientID, event)
			case sse.PublishTargetGroup:
				hub.SendToGroup(m.Group, event)
			case sse.PublishTargetBroadcastExcept:
				hub.BroadcastExcept(event, m.ExcludeIDs...)
			default:
				b.logger.Warn("sse redis bus: unknown target", "target", m.Target)
			}
		}
	}
}

func unixNanoToTime(v int64) time.Time {
	if v <= 0 {
		return time.Now()
	}
	return time.Unix(0, v)
}
