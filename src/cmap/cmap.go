package cmap

import (
	"math"
	"sync/atomic"
)

type ConcurrentMap interface {
	// returns the concurrency
	Concurrency() int

	// put a kv into map.
	// the first return value represents if kv is added successfully.
	// the old elem will be replaced by the new elem if the key is existed.
	Put(key string, elem interface{}) (bool, error)

	// get the elem by key.
	// nil represents the key does not exist.
	Get(key string) interface{}

	// delete the target kv.
	// true represents the kv is existed and is deleted.
	Delete(key string) bool

	// returns the size of map.
	Len() uint64
}

// Implementation of ConcurrentMap interface
type ConcurrentMapImpl struct {
	concurrency int
	segments    []Segment
	total       uint64
}

func NewConcurrentMap(concurrency int, pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency must be greater than 0")
	}
	if concurrency > MaxConcurrency {
		return nil, newIllegalParameterError("concurrency must be smaller than MAX_CONCURRENCY")
	}
	cmap := &ConcurrentMapImpl{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] = newSegment(DefaultBucketNum, pairRedistributor)
	}
	return cmap, nil
}

func (cmap *ConcurrentMapImpl) Concurrency() int {
	return cmap.concurrency
}

func (cmap *ConcurrentMapImpl) Put(key string, elem interface{}) (bool, error) {
	p, err := newPair(key, elem)
	if err != nil {
		return false, err
	}
	s := cmap.findSegment(p.Hash())
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&cmap.total, 1)
	}
	return ok, err
}

func (cmap *ConcurrentMapImpl) Get(key string) interface{} {
	keyHash := hash(key)
	s := cmap.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}
	return pair.Element()
}

func (cmap *ConcurrentMapImpl) Delete(key string) bool {
	s := cmap.findSegment(hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&cmap.total, ^uint64(0))
		return true
	}
	return false
}

func (cmap *ConcurrentMapImpl) Len() uint64 {
	return atomic.LoadUint64(&cmap.total)
}

func (cmap *ConcurrentMapImpl) findSegment(keyHash uint64) Segment {
	if cmap.concurrency == 1 {
		return cmap.segments[0]
	}
	var keyHash32 uint32
	if keyHash > math.MaxUint32 {
		keyHash32 = uint32(keyHash >> 32)
	} else {
		keyHash32 = uint32(keyHash)
	}
	return cmap.segments[int(keyHash32>>16)%(cmap.concurrency-1)]
}
