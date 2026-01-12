package errorpubsubio

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub/v2"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x0[context.Context, *pubsub.PublishResult](&logHandler{})
}

func LogHandler(s beam.Scope, size int, pbResult beam.PCollection) {
	s = s.Scope("arqbeam.pubsubio.LogHandler")

	beam.ParDo0(s, &logHandler{
		Size: size,
	}, pbResult)
}

type logHandler struct {
	Size int

	resultChan chan *pubsub.PublishResult
	wg         sync.WaitGroup
}

func (fn *logHandler) Setup(ctx context.Context) {
	fn.resultChan = make(chan *pubsub.PublishResult, fn.Size*3)

	go func() {
		for result := range fn.resultChan {
			id, err := result.Get(ctx)
			if err != nil {
				log.Errorf(ctx, "Error in %v: %v", id, err.Error())
			} else {
				log.Infof(ctx, "Message processed: %v", id)
			}
			fn.wg.Done()
		}
	}()
}

func (fn *logHandler) ProcessElement(ctx context.Context, r *pubsub.PublishResult) {
	fn.wg.Add(1)
	fn.resultChan <- r
}

func (fn *logHandler) FinishBundle() {
	fn.wg.Wait()
}
