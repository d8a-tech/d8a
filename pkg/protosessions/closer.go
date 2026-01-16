package protosessions

import (
	"hash/fnv"
	"sync"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"
)

// Closer defines an interface for closing and processing hit sessions
type Closer interface {
	Close(protosession [][]*hits.Hit) error
}

type printingCloser struct {
}

// Close implements Closer
func (c *printingCloser) Close(protosessions [][]*hits.Hit) error {
	for _, protosession := range protosessions {
		for _, hit := range protosession {
			logrus.Warnf("Closing protosession: /%v/%v/%v", hit.AuthoritativeClientID, hit.ClientID, hit.ID)
		}
	}
	return nil
}

// NewPrintingCloser creates a new Closer implementation that prints the hits to stdout
func NewPrintingCloser() Closer {
	return &printingCloser{}
}

type shardingCloser struct {
	children []Closer
}

// Close implements Closer
func (c *shardingCloser) Close(protosessions [][]*hits.Hit) error {
	shards := make([][][]*hits.Hit, len(c.children))
	for i := range shards {
		shards[i] = make([][]*hits.Hit, 0)
	}

	for _, protosession := range protosessions {
		if len(protosession) == 0 {
			continue
		}
		idx := c.shardFor(GetIsolatedClientID(protosession[0]))
		shards[idx] = append(shards[idx], protosession)
	}

	var wg sync.WaitGroup
	errs := make([]error, len(c.children))

	for i, child := range c.children {
		if len(shards[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, closer Closer, batch [][]*hits.Hit) {
			defer wg.Done()
			errs[idx] = closer.Close(batch)
		}(i, child, shards[i])
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *shardingCloser) shardFor(clientID hits.ClientID) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	return int(h.Sum32() % util.SafeIntToUint32(len(c.children)))
}

// NewShardingCloser distributes batches of proto-sessions across N child Closers
func NewShardingCloser(n int, factory func(shardIndex int) Closer) Closer {
	children := make([]Closer, n)
	for i := 0; i < n; i++ {
		children[i] = factory(i)
	}
	return &shardingCloser{children: children}
}
