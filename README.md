# go-casbin-kafka-adapter

基于 Apache Kafka 的策略变更通知适配器，用于分布式环境下的策略同步

## 功能特性

- 🔄 **分布式策略同步**：A 节点修改策略后，通过 Kafka 广播变更事件，B/C/D 节点自动重载
- 💾 **消息持久化**：Kafka 保证消息不丢失，支持消息回溯
- 👥 **消费者组**：每个节点使用独立的消费者组，确保消息被正确消费
- 🔁 **自动重连**：Kafka 连接断开后自动重试
- 🚫 **消息去重**：基于事件 ID 和来源节点过滤重复/自身事件
- 🔁 **发布重试**：发布失败时自动重试

## 安装

```bash
go get github.com/kamalyes/go-casbin-kafka-adapter
```

## 基本使用

```go
package main

import (
    "github.com/kamalyes/go-casbin-kafka-adapter"
    "github.com/kamalyes/go-casbin/enforcer"
    "github.com/kamalyes/go-casbin/policy"
    "github.com/kamalyes/go-logger"
)

func main() {
    log := logger.NewLogger().WithLevel(logger.INFO)

    // 创建 Kafka 通知器
    notifier, _ := kafkaadapter.NewKafkaNotifier(
        &kafkaadapter.KafkaConfig{
            Brokers: []string{"localhost:9092"},
            GroupID: "casbin-policy-node-1",
        },
        policy.WithChannel("casbin-policy-changes"),
        policy.WithSource("node-1"),
    )

    // 创建执行器并集成 Kafka 通知器
    e, _ := enforcer.NewEnforcer(
        enforcer.WithModelPath("resources/rbac_model.conf"),
        enforcer.WithPolicyPath("resources/rbac_policy.csv"),
        enforcer.WithNotifier(notifier),
        enforcer.WithLogger(log),
    )
    defer e.Close()

    // 修改策略后自动通知其他节点
    _ = e.AddPolicy("alice", "data3", "read")
}
```

## 多租户使用

每个租户使用独立的 Kafka Topic，实现策略变更通知隔离：

```go
// 租户1：独立 Topic
notifier1, _ := kafkaadapter.NewKafkaNotifier(
    &kafkaadapter.KafkaConfig{
        Brokers: []string{"localhost:9092"},
        GroupID: "casbin-tenant1-node-1",
    },
    policy.WithChannel("casbin-tenant1-policy-changes"),
    policy.WithSource("tenant1-node-1"),
)

e1, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/rbac_with_domains_model.conf"),
    enforcer.WithPolicyPath("resources/rbac_with_domains_policy.csv"),
    enforcer.WithNotifier(notifier1),
    enforcer.WithLogger(log),
)

// 租户2：独立 Topic
notifier2, _ := kafkaadapter.NewKafkaNotifier(
    &kafkaadapter.KafkaConfig{
        Brokers: []string{"localhost:9092"},
        GroupID: "casbin-tenant2-node-1",
    },
    policy.WithChannel("casbin-tenant2-policy-changes"),
    policy.WithSource("tenant2-node-1"),
)

e2, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier2),
    enforcer.WithLogger(log),
)
```

## ABAC + Kafka 分布式同步

ABAC 规则策略变更时，通过 Kafka 广播到所有节点：

```go
notifier, _ := kafkaadapter.NewKafkaNotifier(
    &kafkaadapter.KafkaConfig{
        Brokers: []string{"kafka-1:9092", "kafka-2:9092"},
        GroupID: "casbin-abac-node-1",
    },
    policy.WithChannel("casbin-abac-policy-changes"),
)

e, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier),
    enforcer.WithLogger(log),
)

// 添加 ABAC 规则策略 → 自动通知所有节点
_ = e.AddPolicy(`r.sub == "bob"`, "data2", "write")
```

## 配置说明

### KafkaConfig

| 参数 | 类型 | 说明 |
|------|------|------|
| `Brokers` | `[]string` | Kafka Broker 地址列表 |
| `Version` | `sarama.KafkaVersion` | Kafka 版本（默认使用 MaxVersion） |
| `GroupID` | `string` | 消费者组 ID（每个节点使用不同的 GroupID） |

### NotifierConfig（通过 policy.NotifierOption 配置）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `Channel` | `casbin:policy:changes` | Kafka Topic 名称 |
| `Source` | 自动生成 | 本节点标识 |
| `BufferSize` | `256` | 事件缓冲区大小 |
| `RetryInterval` | `1s` | 发布失败重试间隔 |
| `RetryCount` | `3` | 发布失败重试次数 |

## License

Apache-2.0
