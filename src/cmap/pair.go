package cmap

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"
)

type linkedPair interface {
	Next() Pair
	SetNext(nextPair Pair) error
}

type Pair interface {
	linkedPair
	Key() string
	Hash() uint64
	Element() interface{}
	SetElement(elem interface{}) error
	Copy() Pair
	String() string
}

type pair struct {
	key  string
	hash uint64
	elem unsafe.Pointer
	next unsafe.Pointer
}

func newPair(key string, element interface{}) (Pair, error) {
	p := &pair{
		key:  key,
		hash: hash(key),
	}
	if element == nil {
		return nil, newIllegalParameterError("element must not be nil")
	}
	p.elem = unsafe.Pointer(&element)
	return p, nil
}

func (p *pair) Key() string {
	return p.key
}

func (p *pair) Hash() uint64 {
	return p.hash
}

func (p *pair) Element() interface{} {
	pointer := atomic.LoadPointer(&p.elem)
	if pointer == nil {
		return nil
	}
	return *(*interface{})(pointer)
}

func (p *pair) SetElement(element interface{}) error {
	if element == nil {
		return newIllegalParameterError("element must not be nil")
	}
	atomic.StorePointer(&p.elem, unsafe.Pointer(&element))
	return nil
}

func (p *pair) Next() Pair {
	pointer := atomic.LoadPointer(&p.next)
	if pointer == nil {
		return nil
	}
	return (*pair)(pointer)
}

func (p *pair) SetNext(nextPair Pair) error {
	if nextPair == nil {
		atomic.StorePointer(&p.next, nil)
		return nil
	}
	pp, ok := nextPair.(*pair)
	if !ok {
		return newIllegalPairTypeError(nextPair)
	}
	atomic.StorePointer(&p.next, unsafe.Pointer(pp))
	return nil
}

func (p *pair) Copy() Pair {
	pCopy, _ := newPair(p.Key(), p.Element())
	return pCopy
}

func (p *pair) String() string {
	return p.genString(false)
}

func (p *pair) genString(nextDetail bool) string {
	var buf bytes.Buffer
	buf.WriteString("pair{key:")
	buf.WriteString(p.Key())
	buf.WriteString(", hash:")
	buf.WriteString(fmt.Sprintf("%d", p.Hash()))
	buf.WriteString(", element:")
	buf.WriteString(fmt.Sprintf("%+v", p.Element()))
	if nextDetail {
		buf.WriteString(", next:")
		if next := p.Next(); next != nil {
			if npp, ok := next.(*pair); ok {
				buf.WriteString(npp.genString(nextDetail))
			} else {
				buf.WriteString("<ignore>")
			}
		}
	} else {
		buf.WriteString(", nextKey:")
		if next := p.Next(); next != nil {
			buf.WriteString(next.Key())
		}
	}
	buf.WriteString("}")
	return buf.String()
}
