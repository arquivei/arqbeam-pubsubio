package pubsubio

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[context.Context, []byte, *pubsub.PublishResult](&Publisher{})
}

type PubsubResultError struct {
	MessageID    string
	ErrorMessage string
}

func Publish(s beam.Scope, project string, topic string, batchSize int, targetData beam.PCollection) beam.PCollection {
	s = s.Scope("arqbeam.pubsubio.Publish")

	return beam.ParDo(s, &Publisher{
		Project:   project,
		Topic:     topic,
		BatchSize: batchSize,
	}, targetData)
}

type Publisher struct {
	Project   string
	Topic     string
	BatchSize int

	topic *pubsub.Publisher
}

func (fn *Publisher) Setup(ctx context.Context) {
	pubSubClient, err := pubsub.NewClient(ctx, fn.Project)
	if err != nil {
		panic(err)
	}

	pubSubTopic := pubSubClient.Publisher(fn.Topic)
	pubSubTopic.PublishSettings.CountThreshold = fn.BatchSize
	fn.topic = pubSubTopic
}

func (fn *Publisher) ProcessElement(ctx context.Context, value []byte) *pubsub.PublishResult {
	return fn.topic.Publish(ctx, &pubsub.Message{Data: value})
}
