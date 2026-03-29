/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-kafka-adapter\publish.go
 * @Description: Kafka 发布策略变更事件
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package kafkaadapter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
)

// Publish 发布策略变更事件到 Kafka Topic
func (kn *KafkaNotifier) Publish(ctx context.Context, event *ChangeEvent) error {
	event.ID = kn.idgen.GenerateRequestID()
	event.Source = kn.config.Source
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return errors.NewPolicyWatchFailedError("failed to marshal event: " + err.Error())
	}

	msg := &sarama.ProducerMessage{
		Topic: kn.config.Channel,
		Key:   sarama.StringEncoder(event.ID),
		Value: sarama.ByteEncoder(data),
	}

	var publishErr error
	retryErr := kn.retry.Do(func() error {
		_, _, publishErr = kn.producer.SendMessage(msg)
		return publishErr
	})

	if retryErr != nil {
		return errors.NewPolicyWatchFailedError("failed to publish event to kafka: " + retryErr.Error())
	}

	kn.logger.DebugKV("Policy change event published to Kafka",
		"topic", kn.config.Channel,
		"event_type", string(event.Type),
		"source", event.Source,
	)

	return nil
}

// PublishPolicyAdded 发布策略添加事件
func (kn *KafkaNotifier) PublishPolicyAdded(ctx context.Context, ptype string, p []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyAdded, ptype, kn.config.Source)
	event.NewPolicy = p
	return kn.Publish(ctx, event)
}

// PublishPolicyRemoved 发布策略删除事件
func (kn *KafkaNotifier) PublishPolicyRemoved(ctx context.Context, ptype string, oldPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyRemoved, ptype, kn.config.Source)
	event.OldPolicy = oldPolicy
	return kn.Publish(ctx, event)
}

// PublishPolicyUpdated 发布策略更新事件
func (kn *KafkaNotifier) PublishPolicyUpdated(ctx context.Context, ptype string, oldPolicy, newPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyUpdated, ptype, kn.config.Source)
	event.OldPolicy = oldPolicy
	event.NewPolicy = newPolicy
	return kn.Publish(ctx, event)
}

// PublishPolicyReload 发布策略全量重载事件
func (kn *KafkaNotifier) PublishPolicyReload(ctx context.Context) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyReload, "", kn.config.Source)
	return kn.Publish(ctx, event)
}
