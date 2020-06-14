package cmap

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// do not send lock if cmap is already locked
type Bucket interface {
	Put(p Pair, lock sync.Locker) (bool, error)
	Get(key string) Pair
	GetFirstPair() Pair
	Delete(key string, lock sync.Locker) bool
	Clear(lock sync.Locker)
	Size() uint64
	String() string
}

type bucket struct {
	headValue atomic.Value
	size      uint64
}

var placeholder Pair = &pair{}

func newBucket() Bucket {
	b := &bucket{}
	b.headValue.Store(placeholder)
	return b
}

func (b *bucket) Put(p Pair, lock sync.Locker) (bool, error) {
	if p == nil {
		return false, newIllegalParameterError("pair is nil")
	}

	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	// empty bucket
	headPair := b.GetFirstPair()
	if headPair == nil {
		b.headValue.Store(p)
		atomic.AddUint64(&b.size, 1)
		return true, nil
	}

	var target Pair
	key := p.Key()
	for v := headPair; v != nil; v = v.Next() {
		if v.Key() == key {
			target = v
			break
		}
	}
	// pair existed
	if target != nil {
		err := target.SetElement(p.Element())
		return false, err
	}

	// insert new pair from head
	err := p.SetNext(headPair)
	b.headValue.Store(p)
	atomic.AddUint64(&b.size, 1)
	return true, err
}

func (b *bucket) Get(key string) Pair {
	headPair := b.GetFirstPair()
	if headPair == nil {
		return nil
	}
	for v := headPair; v != nil; v = v.Next() {
		if v.Key() == key {
			return v
		}
	}
	return nil
}

func (b *bucket) GetFirstPair() Pair {
	if v := b.headValue.Load(); v == nil {
		return nil
	} else if p, ok := v.(Pair); !ok || p == placeholder {
		return nil
	} else {
		return p
	}
}

func (b *bucket) Delete(key string, lock sync.Locker) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	headPair := b.GetFirstPair()
	if headPair == nil {
		return false
	}

	var prevPairs []Pair
	var target Pair
	var breakpoint Pair
	for v := headPair; v != nil; v = v.Next() {
		if v.Key() == key {
			target = v
			breakpoint = v.Next()
			break
		}
		prevPairs = append(prevPairs, v)
	}

	if target == nil {
		return false
	}

	newHeadPair := breakpoint
	for i := len(prevPairs) - 1; i >= 0; i-- {
		pairCopy := prevPairs[i].Copy()
		_ = pairCopy.SetNext(newHeadPair)
		newHeadPair = pairCopy
	}

	if newHeadPair != nil {
		b.headValue.Store(newHeadPair)
	} else {
		b.headValue.Store(placeholder)
	}
	atomic.AddUint64(&b.size, ^uint64(0))
	return true
}

func (b *bucket) Clear(lock sync.Locker) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	atomic.StoreUint64(&b.size, 0)
	b.headValue.Store(placeholder)
}

func (b *bucket) Size() uint64 {
	return atomic.LoadUint64(&b.size)
}

func (b *bucket) String() string {
	var buf bytes.Buffer
	buf.WriteString("[ ")
	for v := b.GetFirstPair(); v != nil; v = v.Next() {
		buf.WriteString(v.String())
		buf.WriteString(" ")
	}
	buf.WriteString("]")
	return buf.String()
}
