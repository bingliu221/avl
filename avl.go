package avl

import (
	"sync"

	"golang.org/x/exp/constraints"
)

type ordered = constraints.Ordered

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type node[K ordered, V any] struct {
	key   K
	value V

	height int

	left  *node[K, V]
	right *node[K, V]
}

func (t *node[K, V]) updateHeight() {
	if t == nil {
		return
	}
	t.height = 1 + max(t.left._height(), t.right._height())
}

func (t *node[K, V]) _height() int {
	if t == nil {
		return 0
	}
	return t.height
}

func rotateLeft[K ordered, V any](pn **node[K, V]) {
	root := (*pn)
	*pn = (*pn).right
	root.right = (*pn).left
	(*pn).left = root
	root.updateHeight()
	(*pn).updateHeight()
}

func rotateRight[K ordered, V any](pn **node[K, V]) {
	root := (*pn)
	*pn = (*pn).left
	root.left = (*pn).right
	(*pn).right = root
	root.updateHeight()
	(*pn).updateHeight()
}

func balance[K ordered, V any](pn **node[K, V]) {
	t := *pn
	if t == nil {
		return
	}
	if abs(t.left._height()-t.right._height()) <= 1 {
		return
	}
	if t.left._height() > t.right._height() {
		if t.left.left._height() < t.left.right._height() {
			rotateLeft(&t.left)
		}
		rotateRight(pn)
	} else {
		if t.right.left._height() > t.right.right._height() {
			rotateRight(&t.right)
		}
		rotateLeft(pn)
	}
}

func mostLeft[K ordered, V any](pn **node[K, V]) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	if t.left == nil {
		return t
	}
	if t.left.left == nil {
		return t.left
	}
	return mostLeft(&t.left)
}

func mostRight[K ordered, V any](pn **node[K, V]) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	if t.right == nil {
		return t
	}
	if t.right.right == nil {
		return t.right
	}
	return mostRight(&t.right)
}

func popMostLeft[K ordered, V any](pn **node[K, V]) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	if t.left == nil {
		return t
	}
	defer balance(pn)
	if t.left.left == nil {
		mostLeft := t.left
		t.left = t.left.right
		return mostLeft
	}
	return popMostLeft(&t.left)
}

func popMostRight[K ordered, V any](pn **node[K, V]) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	if t.right == nil {
		return t
	}
	defer balance(pn)
	if t.right.right == nil {
		mostRight := t.right
		t.right = t.right.left
		return mostRight
	}
	return popMostRight(&t.right)
}

func insert[K ordered, V any](pn **node[K, V], child *node[K, V]) {
	if *pn == nil {
		*pn = child
		(*pn).updateHeight()
		return
	}

	t := *pn
	switch {
	case child.key == t.key:
		t.value = child.value
	case child.key < t.key:
		insert(&t.left, child)
	case child.key > t.key:
		insert(&t.right, child)
	}

	(*pn).updateHeight()
	balance(pn)
}

func find[K ordered, V any](pn **node[K, V], key K) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	switch {
	case key == t.key:
		return t
	case key < t.key:
		return find(&t.left, key)
	case key > t.key:
		return find(&t.right, key)
	default:
		return nil
	}
}

func remove[K ordered, V any](pn **node[K, V], key K) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}

	switch {
	case key == t.key:
		if t.left._height() > t.right._height() {
			mostRight := popMostRight(&t.left)
			if mostRight != nil {
				mostRight.right = t.right
				*pn = mostRight
			} else {
				*pn = t.right
			}
		} else {
			mostLeft := popMostLeft(&t.right)
			if mostLeft != nil {
				mostLeft.left = t.left
				*pn = mostLeft
			} else {
				*pn = t.left
			}
		}
		t.height = 1
		t.left = nil
		t.right = nil
	case key < t.key:
		t = remove(&t.left, key)
	case key > t.key:
		t = remove(&t.right, key)
	}

	(*pn).updateHeight()
	balance(pn)

	return t
}

func (t *node[K, V]) _range(f func(key K, value V) bool, reverse bool) bool {
	if t == nil {
		return true
	}

	if reverse {
		if ok := t.right._range(f, reverse); !ok {
			return false
		}
	} else {
		if ok := t.left._range(f, reverse); !ok {
			return false
		}
	}

	if ok := f(t.key, t.value); !ok {
		return false
	}

	if reverse {
		if ok := t.left._range(f, reverse); !ok {
			return false
		}
	} else {
		if ok := t.right._range(f, reverse); !ok {
			return false
		}
	}

	return true
}

type Tree[K constraints.Ordered, V any] struct {
	mutex sync.RWMutex
	root  *node[K, V]
}

func New[K constraints.Ordered, V any]() *Tree[K, V] {
	return &Tree[K, V]{}
}

func (t *Tree[K, V]) Store(key K, value V) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	insert(&t.root, &node[K, V]{
		key:   key,
		value: value,
	})
}

func (t *Tree[K, V]) Load(key K) (value V, ok bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	n := find(&t.root, key)
	if n == nil {
		return
	}
	return n.value, true
}

func (t *Tree[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	n := remove(&t.root, key)
	if n == nil {
		return
	}
	return n.value, true
}

func (t *Tree[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	n := find(&t.root, key)
	if n == nil {
		insert(&t.root, &node[K, V]{
			key:   key,
			value: value,
		})
		return value, false
	}
	return n.value, true
}

func (t *Tree[K, V]) LoadOrStoreCreate(key K, create func() V) (actual V, loaded bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	n := find(&t.root, key)
	if n == nil {
		value := create()
		insert(&t.root, &node[K, V]{
			key:   key,
			value: value,
		})
		return value, false
	}
	return n.value, true
}

func (t *Tree[K, V]) Delete(key K) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	remove(&t.root, key)
}

func (t *Tree[K, V]) Range(f func(key K, value V) bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	t.root._range(f, false)
}

func (t *Tree[K, V]) RangeReverse(f func(key K, value V) bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	t.root._range(f, true)
}

func (t *Tree[K, V]) First() (key K, value V, exists bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	n := mostLeft(&t.root)
	if n == nil {
		return
	}
	return n.key, n.value, true
}

func (t *Tree[K, V]) Last() (key K, value V, exists bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	n := mostRight(&t.root)
	if n == nil {
		return
	}
	return n.key, n.value, true
}
