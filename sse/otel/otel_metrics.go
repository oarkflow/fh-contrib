package sseotel

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	sse "github.com/oarkflow/sse"
)

func RegisterOTelMetrics(meter metric.Meter, hub *sse.Hub, handler *sse.HandlerMetrics) (func(), error) {
	connectedClients, err := meter.Int64ObservableGauge("sse.connected_clients")
	if err != nil {
		return nil, err
	}
	deliveredEvents, err := meter.Int64ObservableCounter("sse.hub.delivered_events")
	if err != nil {
		return nil, err
	}
	droppedEvents, err := meter.Int64ObservableCounter("sse.hub.dropped_events")
	if err != nil {
		return nil, err
	}
	replayedEvents, err := meter.Int64ObservableCounter("sse.hub.replayed_events")
	if err != nil {
		return nil, err
	}
	activeConnections, err := meter.Int64ObservableGauge("sse.handler.active_connections")
	if err != nil {
		return nil, err
	}
	authFailures, err := meter.Int64ObservableCounter("sse.handler.auth_failures")
	if err != nil {
		return nil, err
	}

	reg, err := meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		if hub != nil {
			s := hub.Stats()
			obs.ObserveInt64(connectedClients, int64(s.ConnectedClients))
			obs.ObserveInt64(deliveredEvents, int64(s.DeliveredEvents))
			obs.ObserveInt64(droppedEvents, int64(s.DroppedEvents))
			obs.ObserveInt64(replayedEvents, int64(s.ReplayedEvents))
		}
		if handler != nil {
			s := handler.Snapshot()
			obs.ObserveInt64(activeConnections, s.ActiveConnections)
			obs.ObserveInt64(authFailures, int64(s.AuthFailures))
		}
		return nil
	}, connectedClients, deliveredEvents, droppedEvents, replayedEvents, activeConnections, authFailures)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}
