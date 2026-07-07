package sseredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPresenceStore struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

type RedisPresenceStoreOptions struct {
	Prefix string
	TTL    time.Duration
}

func NewRedisPresenceStore(client *redis.Client, opts RedisPresenceStoreOptions) *RedisPresenceStore {
	if opts.Prefix == "" {
		opts.Prefix = "sse:presence:"
	}
	if opts.TTL <= 0 {
		opts.TTL = 10 * time.Minute
	}
	return &RedisPresenceStore{
		client: client,
		prefix: opts.Prefix,
		ttl:    opts.TTL,
	}
}

func (s *RedisPresenceStore) userKey(userID string) string {
	return s.prefix + "user:" + userID
}

func (s *RedisPresenceStore) usersKey() string {
	return s.prefix + "users"
}

func (s *RedisPresenceStore) Add(userID, clientID string) error {
	ctx := context.Background()
	userKey := s.userKey(userID)
	pipe := s.client.Pipeline()
	pipe.SAdd(ctx, userKey, clientID)
	pipe.Expire(ctx, userKey, s.ttl)
	pipe.SAdd(ctx, s.usersKey(), userID)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisPresenceStore) Remove(userID, clientID string) error {
	ctx := context.Background()
	userKey := s.userKey(userID)
	pipe := s.client.Pipeline()
	pipe.SRem(ctx, userKey, clientID)
	countCmd := pipe.SCard(ctx, userKey)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if countCmd.Val() == 0 {
		pipe = s.client.Pipeline()
		pipe.Del(ctx, userKey)
		pipe.SRem(ctx, s.usersKey(), userID)
		_, _ = pipe.Exec(ctx)
	}
	return nil
}

func (s *RedisPresenceStore) IsOnline(userID string) (bool, error) {
	ctx := context.Background()
	n, err := s.client.SCard(ctx, s.userKey(userID)).Result()
	return n > 0, err
}

func (s *RedisPresenceStore) ConnectionCount(userID string) (int, error) {
	ctx := context.Background()
	n, err := s.client.SCard(ctx, s.userKey(userID)).Result()
	return int(n), err
}

func (s *RedisPresenceStore) OnlineUsers() ([]string, error) {
	ctx := context.Background()
	return s.client.SMembers(ctx, s.usersKey()).Result()
}
