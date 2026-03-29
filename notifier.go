/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-kafka-adapter\notifier.go
 * @Description: Kafka 通知器核心结构体与构造函数
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package kafkaadapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/idgen"
	"github.com/kamalyes/go-toolbox/pkg/retry"
)

// 编译时接口断言
var _ policy.PolicyNotifier = (*KafkaNotifier)(nil)

// KafkaNotifier 基于 Kafka 的策略变更通知器
// 使用 Kafka Topic 作为发布/订阅频道，实现分布式策略同步
// 适用于大规模分布式部署、跨数据中心同步等场景
//
// 支持特性：
//   - 持久化消息：Kafka 保证消息不丢失，支持消息回溯
//   - 消费者组：每个节点使用独立的消费者组，确保每条消息只被同组一个消费者处理
//   - 自动重连：Kafka 连接断开后自动重试
//   - 消息去重：基于事件 ID 和来源节点过滤重复/自身事件
//   - 发布重试：发布失败时自动重试
type KafkaNotifier struct {
	producer sarama.SyncProducer    // Kafka 同步生产者
	consumer sarama.ConsumerGroup   // Kafka 消费者组
	config   *policy.NotifierConfig // 通知器配置
	logger   logger.ILogger         // 日志记录器
	idgen    idgen.IDGenerator      // ID 生成器
	retry    *retry.Retry           // 发布重试器

	mu      sync.RWMutex              // 保护以下字段
	running bool                      // 是否正在运行
	cancel  context.CancelFunc        // 取消消费的函数
	handler policy.ChangeEventHandler // 事件处理函数
}

// NewKafkaNotifier 创建 Kafka 通知器
func NewKafkaNotifier(kafkaConfig *KafkaConfig, opts ...policy.NotifierOption) (*KafkaNotifier, error) {
	if len(kafkaConfig.Brokers) == 0 {
		return nil, errors.NewPolicyAdapterFailedError("kafka brokers is empty")
	}

	config := policy.DefaultNotifierConfig()
	for _, opt := range opts {
		opt(config)
	}

	if config.Source == "unknown" {
		config.Source = fmt.Sprintf("node-%s", idgen.NewIDGenerator(idgen.GeneratorTypeUUID).GenerateRequestID())
	}

	saramaConfig := sarama.NewConfig()
	if kafkaConfig.Version.IsAtLeast(sarama.MinVersion) {
		saramaConfig.Version = kafkaConfig.Version
	} else {
		saramaConfig.Version = sarama.MaxVersion
	}
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(kafkaConfig.Brokers, saramaConfig)
	if err != nil {
		return nil, errors.NewPolicyAdapterFailedError("failed to create kafka producer: " + err.Error())
	}

	groupID := kafkaConfig.GroupID
	if groupID == "" {
		groupID = "casbin-policy-" + config.Source
	}
	consumer, err := sarama.NewConsumerGroup(kafkaConfig.Brokers, groupID, saramaConfig)
	if err != nil {
		_ = producer.Close()
		return nil, errors.NewPolicyAdapterFailedError("failed to create kafka consumer group: " + err.Error())
	}

	return &KafkaNotifier{
		producer: producer,
		consumer: consumer,
		config:   config,
		logger:   logger.NewEmptyLogger(),
		idgen:    idgen.NewIDGenerator(idgen.GeneratorTypeUUID),
		retry: retry.NewRetry().
			SetAttemptCount(config.RetryCount).
			SetInterval(config.RetryInterval),
	}, nil
}

// Close 关闭通知器
func (kn *KafkaNotifier) Close() error {
	_ = kn.Unsubscribe()
	if kn.producer != nil {
		_ = kn.producer.Close()
	}
	if kn.consumer != nil {
		_ = kn.consumer.Close()
	}
	return nil
}
