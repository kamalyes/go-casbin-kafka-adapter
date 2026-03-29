/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-kafka-adapter\subscribe.go
 * @Description: Kafka 订阅策略变更事件
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package kafkaadapter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
)

// Subscribe 订阅策略变更事件
func (kn *KafkaNotifier) Subscribe(ctx context.Context, handler policy.ChangeEventHandler) error {
	kn.mu.Lock()
	defer kn.mu.Unlock()

	if kn.running {
		return nil
	}

	kn.handler = handler
	subCtx, cancel := context.WithCancel(ctx)
	kn.cancel = cancel
	kn.running = true

	go kn.consumeLoop(subCtx)

	kn.logger.InfoKV("Kafka notifier subscribed",
		"topic", kn.config.Channel,
		"source", kn.config.Source,
	)

	return nil
}

// Unsubscribe 取消订阅
func (kn *KafkaNotifier) Unsubscribe() error {
	kn.mu.Lock()
	defer kn.mu.Unlock()

	if !kn.running {
		return nil
	}

	kn.running = false
	if kn.cancel != nil {
		kn.cancel()
	}

	kn.logger.InfoKV("Kafka notifier unsubscribed", "topic", kn.config.Channel)
	return nil
}

// consumeLoop 消费循环
func (kn *KafkaNotifier) consumeLoop(ctx context.Context) {
	handler := &kafkaConsumerHandler{
		source: kn.config.Source,
		handler: func(event *ChangeEvent) {
			kn.mu.RLock()
			h := kn.handler
			kn.mu.RUnlock()
			if h != nil {
				h(event)
			}
		},
		logger: kn.logger,
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		topics := []string{kn.config.Channel}
		if err := kn.consumer.Consume(ctx, topics, handler); err != nil {
			kn.logger.WarnKV("Kafka consumer error, reconnecting...",
				"topic", kn.config.Channel,
				"error", err.Error(),
			)
			time.Sleep(kn.config.RetryInterval)
		}
	}
}

// kafkaConsumerHandler Sarama 消费者组处理器
type kafkaConsumerHandler struct {
	source  string
	handler func(*ChangeEvent)
	logger  logger.ILogger
}

func (h *kafkaConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *kafkaConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *kafkaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event ChangeEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			h.logger.WarnKV("Failed to unmarshal kafka event", "error", err.Error())
			session.MarkMessage(msg, "")
			continue
		}

		if event.Source == h.source {
			h.logger.DebugKV("Skipping self-published event",
				"event_type", string(event.Type),
				"source", event.Source,
			)
			session.MarkMessage(msg, "")
			continue
		}

		h.handler(&event)
		session.MarkMessage(msg, "")
	}
	return nil
}
