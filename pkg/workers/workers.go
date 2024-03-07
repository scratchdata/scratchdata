package workers

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"sync"
)

type ScratchDataWorker struct {
	Config          config.Workers
	StorageServices storage.StorageServices
}

func (w *ScratchDataWorker) Start(ctx context.Context, threadId int) {
	// defer wg.Done()

	log.Debug().Int("thread", threadId).Msg("Starting worker")

	for {
		select {
		case <-ctx.Done():
			log.Debug().Int("thread", threadId).Msg("Stopping worker")
			return
			//default:
			//	for i := 0; i < 5; i++ {
			//		time.Sleep(1 * time.Second)
			//		log.Debug().Int("i", i).Int("thread", threadId).Msg("Doing work")
			//	}
		}
	}
}

func RunWorkers(ctx context.Context, config config.Workers, storageServices storage.StorageServices) {
	workers := &ScratchDataWorker{
		Config:          config,
		StorageServices: storageServices,
	}

	log.Debug().Msg("Starting Workers")
	var wg sync.WaitGroup
	i := 0
	for i = 0; i < config.Count; i++ {
		wg.Add(1)
		go func(threadId int) {
			defer wg.Done()
			workers.Start(ctx, threadId)
		}(i)
	}
	wg.Wait()

	// Clean up resources and gracefully shut down the web server
}
