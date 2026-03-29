/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin-kafka-adapter\config.go
 * @Description: Kafka 适配器配置定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package kafkaadapter

import (
	"github.com/IBM/sarama"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
)

// KafkaConfig Kafka 连接配置
type KafkaConfig struct {
	Brokers []string            // Kafka Broker 地址列表
	Version sarama.KafkaVersion // Kafka 版本（范围 MinVersion < X > MaxVersion）
	GroupID string              // 消费者组 ID（每个节点使用不同的 GroupID）
}

// ChangeEvent Kafka 适配器事件类型（与 policy.ChangeEvent 一致）
type ChangeEvent = policy.ChangeEvent

// WithNotifierLogger 设置通知器日志记录器
func WithNotifierLogger(l logger.ILogger) func(*KafkaNotifier) {
	return func(kn *KafkaNotifier) {
		if l != nil {
			kn.logger = l
		}
	}
}
