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
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
)

// Subscribe 订阅策略变更事件
// 启动 syncx.EventLoop 异步处理事件，consumeLoop 只负责接收和非阻塞投递
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

	// 启动事件处理循环（syncx.EventLoop，内置 panic 恢复和优雅关闭）
	kn.startEventLoop()

	// 启动消费循环（syncx.Go 统一 panic 恢复，consumeLoop 内部阻塞调用 Consume 不适合 EventLoop）
	syncx.Go(subCtx).
		OnPanic(func(r interface{}) {
			kn.logger.ErrorKV("Kafka consumeLoop panic recovered",
				"panic", fmt.Sprintf("%v", r))
		}).
		Exec(func() { kn.consumeLoop(subCtx) })

	kn.logger.InfoKV("Kafka notifier subscribed",
		"topic", kn.config.Channel,
		"source", kn.config.Source,
	)

	return nil
}

// Unsubscribe 取消订阅
func (kn *KafkaNotifier) Unsubscribe() error {
	kn.mu.Lock()

	if !kn.running {
		kn.mu.Unlock()
		return nil
	}

	kn.running = false
	if kn.cancel != nil {
		kn.cancel()
	}

	kn.mu.Unlock()

	// 取消事件处理循环并等待退出
	kn.workerCancel()
	kn.wg.Wait()

	kn.logger.InfoKV("Kafka notifier unsubscribed", "topic", kn.config.Channel)
	return nil
}

// startEventLoop 启动事件处理循环
// 使用 syncx.EventLoop 替代手写 select+recover，统一并发原语
func (kn *KafkaNotifier) startEventLoop() {
	kn.wg.Add(1)
	go func() {
		defer kn.wg.Done()
		syncx.NewEventLoop(kn.workerCtx).
			OnChannel(kn.eventCh, func(event *ChangeEvent) {
				kn.mu.RLock()
				h := kn.handler
				kn.mu.RUnlock()
				if h != nil {
					h(event)
				}
			}).
			OnPanic(func(r interface{}) {
				kn.logger.ErrorKV("Kafka event handler panic recovered",
					"panic", fmt.Sprintf("%v", r))
			}).
			OnShutdown(func() {
				kn.logger.InfoKV("Kafka event loop shutdown")
			}).
			Run()
	}()
}

// consumeLoop 消费循环
// 使用 select+timer 替代 time.Sleep，context 感知重连等待
func (kn *KafkaNotifier) consumeLoop(ctx context.Context) {
	handler := &kafkaConsumerHandler{
		source:  kn.config.Source,
		eventCh: kn.eventCh,
		logger:  kn.logger,
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
			// context 感知的重连等待，替代 time.Sleep
			select {
			case <-ctx.Done():
				return
			case <-time.After(kn.config.RetryInterval):
			}
		}
	}
}

// kafkaConsumerHandler Sarama 消费者组处理器
type kafkaConsumerHandler struct {
	source  string
	eventCh chan *ChangeEvent
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

		// 非阻塞投递到 eventCh，缓冲区满时丢弃并告警（不阻塞消费）
		select {
		case h.eventCh <- &event:
		default:
			h.logger.WarnKV("Kafka event buffer full, dropping event",
				"event_type", string(event.Type),
				"source", event.Source,
			)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
