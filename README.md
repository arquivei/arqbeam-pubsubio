# arqbeam-sink-pubsub
An Apache Beam sink for arqbeam-app.

This implementation uses go/pubsub google sdk to publish messages in Pubsub topic.

TL;DR

```go
package main

import (
	"context"
    "github.com/arquivei/arqbeam-sink-pubsub"
	errorpubsubio "github.com/arquivei/arqbeam-sink-pubsub/error"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

var (
	pipeline *beam.Pipeline
)

func getPipeline(ctx context.Context) *beam.Pipeline {
	if pipeline != nil {
		return pipeline
	}

	pipeline := beam.NewPipeline()
	s := pipeline.Root()

    // Read some files with textio default from apache beam go sdk 
	readRows := textio.Read(s, config.GCSInputFile)

    // Send each line to pubsub with pubsubio from arqbeam-sink-pubsub
	pbResult := pubsubio.Publish(s, config.Pubsub.Project, config.Pubsub.Topic, config.Pubsub.BatchSize, readRows)

    // Log if any error happened in publish step
	errorpubsubio.LogHandler(s, config.Pubsub.BatchSize, pbResult)

	return pipeline
}

```

Comments, discussions, issues and pull-requests are welcomed.
