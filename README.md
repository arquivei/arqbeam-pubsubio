# arqbeam-pubsubio
An Apache Beam sink for arqbeam-app.

This implementation uses go/pubsub google sdk to publish messages in Pubsub topic.

TL;DR

```go
package main

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/arquivei/beamapp"

	pubsubio "github.com/arquivei/arqbeam-pubsubio"
	errorpubsubio "github.com/arquivei/arqbeam-pubsubio/error"
)

var (
	pipeline *beam.Pipeline
	version  = "dev"
)

var config struct {
	beamapp.Config

	GCSInputFile string
	Pubsub       struct {
		Project   string
		Topic     string
		BatchSize int
	}
}

func main() {
	beamapp.Bootstrap(version, &config)
	pipeline = getPipeline(context.Background())
	beamapp.Run(pipeline)
}

func getPipeline(_ context.Context) *beam.Pipeline {
	if pipeline != nil {
		return pipeline
	}

	pipeline := beam.NewPipeline()
	s := pipeline.Root()

	// Read some files with textio default from apache beam go sdk
	readRows := textio.Read(s, config.GCSInputFile)

	// Send each line to pubsub with pubsubio from arqbeam-pubsubio
	pbResult := pubsubio.Publish(s, config.Pubsub.Project, config.Pubsub.Topic, config.Pubsub.BatchSize, readRows)

	// Log if any error happened in publish step
	errorpubsubio.LogHandler(s, config.Pubsub.BatchSize, pbResult)

	return pipeline
}

```

Comments, discussions, issues and pull-requests are welcomed.
