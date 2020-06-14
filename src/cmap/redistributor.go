package cmap

import (
	"sync/atomic"
)

type BucketStatus uint8

const (
	BucketStatusNormal      BucketStatus = 0
	BucketStatusUnderweight BucketStatus = 1
	BucketStatusOverweight  BucketStatus = 2
)

type PairRedistributor interface {
	UpdateThreshold(pairTotal uint64, bucketNum int)
	CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus)
	Redistribute(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool)
}

type pairRedistributorImpl struct {
	loadFactor            float64
	upperThreshold        uint64 // for bucket
	overweightBucketCount uint64
	emptyBucketCount      uint64
}

func newDefaultPairRedistributor(loadFactor float64, bucketNum int) PairRedistributor {
	if loadFactor <= 0 {
		loadFactor = DefaultBucketLoadFactor
	}
	pr := &pairRedistributorImpl{}
	pr.loadFactor = loadFactor
	pr.UpdateThreshold(0, bucketNum)
	return pr
}

var bucketCountTemplate = `Bucket count:
    pairTotal: %d
    bucketNum: %d
    average: %f
    upperThreshold: %d
    emptyBucketCount: %d`

func (pr *pairRedistributorImpl) UpdateThreshold(pairTotal uint64, bucketNum int) {
	var average float64
	average = float64(pairTotal / uint64(bucketNum))
	if average < 100 {
		average = 100
	}
	//defer func() {
	//    fmt.Printf(bucketCountTemplate,
	//        pairTotal,
	//        bucketNum,
	//        average,
	//        atomic.LoadUint64(&pr.upperThreshold),
	//        atomic.LoadUint64(&pr.emptyBucketCount))
	//}()
	atomic.StoreUint64(&pr.upperThreshold, uint64(average*pr.loadFactor))
}

var bucketStatusTemplate = `Check bucket status:
    pairTotal: %d
    bucketSize: %d
    upperThreshold: %d
    overweightBucketCount: %d
    emptyBucketCount: %d
    bucketStatus: %d`

func (pr *pairRedistributorImpl) CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus) {
	//defer func() {
	//    fmt.Printf(bucketCountTemplate,
	//        pairTotal,
	//        bucketStatus,
	//        atomic.LoadUint64(&pr.upperThreshold),
	//        atomic.LoadUint64(&pr.overweightBucketCount),
	//        atomic.LoadUint64(&pr.emptyBucketCount),
	//        bucketStatus)
	//}()
	if bucketSize > DefaultBucketMaxSize || bucketSize >= atomic.LoadUint64(&pr.upperThreshold) {
		atomic.AddUint64(&pr.overweightBucketCount, 1)
		bucketStatus = BucketStatusOverweight
		return
	}
	if bucketSize == 0 {
		atomic.AddUint64(&pr.emptyBucketCount, 1)
	}
	return
}

var redistributionTemplate = `Redistributing:
    bucketStatus: %d
    currentNumber: %d
    newNumber: %d`

func (pr *pairRedistributorImpl) Redistribute(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool) {
	currentNumber := uint64(len(buckets))
	newNumber := currentNumber
	//defer func() {
	//    fmt.Printf(redistributionTemplate,
	//        bucketStatus,
	//        currentNumber,
	//        newNumber)
	//}()
	switch bucketStatus {
	case BucketStatusOverweight:
		if atomic.LoadUint64(&pr.overweightBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber << 1
	case BucketStatusUnderweight:
		if currentNumber < 100 || atomic.LoadUint64(&pr.emptyBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber >> 1
		if newNumber < 2 {
			newNumber = 2
		}
	default:
		return nil, false
	}
	if newNumber == currentNumber {
		atomic.StoreUint64(&pr.overweightBucketCount, 0)
		atomic.StoreUint64(&pr.emptyBucketCount, 0)
		return nil, false
	}
	var pairs []Pair
	for _, b := range buckets {
		for e := b.GetFirstPair(); e != nil; e = e.Next() {
			pairs = append(pairs, e)
		}
	}
	if newNumber > currentNumber {
		for i := uint64(0); i < currentNumber; i++ {
			buckets[i].Clear(nil)
		}
		for j := newNumber - currentNumber; j > 0; j-- {
			buckets = append(buckets, newBucket())
		}
	} else {
		buckets = make([]Bucket, newNumber)
		for i := uint64(0); i < newNumber; i++ {
			buckets[i] = newBucket()
		}
	}
	var count int
	for _, p := range pairs {
		index := int(p.Hash() % newNumber)
		b := buckets[index]
		_, _ = b.Put(p, nil)
		count++
	}
	atomic.StoreUint64(&pr.overweightBucketCount, 0)
	atomic.StoreUint64(&pr.emptyBucketCount, 0)
	return buckets, true
}
