package cmap

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Segment interface {
	Put(p Pair) (bool, error)
	Get(key string) Pair
	GetWithHash(key string, keyHash uint64) Pair
	Delete(key string) bool
	Size() uint64
}

type segment struct {
	buckets           []Bucket
	bucketsLen        int
	pairTotal         uint64
	pairRedistributor PairRedistributor
	lock              sync.Mutex
}

func newSegment(bucketNum int, pairRedistributor PairRedistributor) Segment {
	if bucketNum <= 0 {
		bucketNum = DefaultBucketNum
	}
	if pairRedistributor == nil {
		pairRedistributor = newDefaultPairRedistributor(DefaultBucketLoadFactor, bucketNum)
	}
	buckets := make([]Bucket, bucketNum)
	for i := 0; i < bucketNum; i++ {
		buckets[i] = newBucket()
	}
	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNum,
		pairRedistributor: pairRedistributor,
	}
}

func (s *segment) Put(p Pair) (bool, error) {
	s.lock.Lock()
	b := s.buckets[int(p.Hash()%uint64(s.bucketsLen))]
	ok, err := b.Put(p, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, 1)
		_ = s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok, err
}

func (s *segment) Get(key string) Pair {
	return s.GetWithHash(key, hash(key))
}

func (s *segment) GetWithHash(key string, keyHash uint64) Pair {
	s.lock.Lock()
	b := s.buckets[int(keyHash%uint64(s.bucketsLen))]
	s.lock.Unlock()
	return b.Get(key)
}

func (s *segment) Delete(key string) bool {
	s.lock.Lock()
	b := s.buckets[int(hash(key)%uint64(s.bucketsLen))]
	ok := b.Delete(key, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, ^uint64(0))
		_ = s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok
}

func (s *segment) redistribute(pairTotal uint64, bucketSize uint64) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if pErr, ok := p.(error); ok {
				err = newPairRedistributorError(pErr.Error())
			} else {
				err = newPairRedistributorError(fmt.Sprintf("%s", p))
			}
		}
	}()
	s.pairRedistributor.UpdateThreshold(pairTotal, s.bucketsLen)
	bucketStatus := s.pairRedistributor.CheckBucketStatus(pairTotal, bucketSize)
	newBuckets, changed := s.pairRedistributor.Redistribute(bucketStatus, s.buckets)
	if changed {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}
	return nil
}

func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.pairTotal)
}
