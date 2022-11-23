package avl

import (
	"errors"
	"sync"

	"golang.org/x/exp/constraints"
)

var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already esists")
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

func popMostLeft[K ordered, V any](pn **node[K, V]) *node[K, V] {
	t := *pn
	if t == nil {
		return nil
	}
	if t.left == nil {
		return t
	}
	if t.left.left == nil {
		mostLeft := t.left
		t.left = nil
		balance(pn)
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
	if t.right.right == nil {
		mostRight := t.right
		t.right = nil
		balance(pn)
		return mostRight
	}
	return popMostRight(&t.right)
}

func insert[K ordered, V any](pn **node[K, V], child *node[K, V]) error {
	if *pn == nil {
		*pn = child
		(*pn).updateHeight()
		return nil
	}

	t := *pn
	switch {
	case child.key == t.key:
		return ErrExists
	case child.key < t.key:
		err := insert(&t.left, child)
		if err != nil {
			return err
		}
	case child.key > t.key:
		err := insert(&t.right, child)
		if err != nil {
			return err
		}
	}

	(*pn).updateHeight()
	balance(pn)
	return nil
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

func remove[K ordered, V any](pn **node[K, V], key K) error {
	t := *pn
	if t == nil {
		return ErrNotFound
	}

	switch {
	case key == t.key:
		if t.left._height() > t.right._height() {
			mostRight := popMostRight(&t.left)
			if mostRight != nil {
				mostRight.left = t.left
				mostRight.right = t.right
				*pn = mostRight
			} else {
				*pn = t.right
			}
		} else {
			mostLeft := popMostLeft(&t.right)
			if mostLeft != nil {
				mostLeft.left = t.left
				mostLeft.right = t.right
				*pn = mostLeft
			} else {
				*pn = t.left
			}
		}
	case key < t.key:
		err := remove(&t.left, key)
		if err != nil {
			return err
		}
	case key > t.key:
		err := remove(&t.right, key)
		if err != nil {
			return err
		}
	}

	(*pn).updateHeight()
	balance(pn)
	return nil
}

func (t *node[K, V]) _range(f func(key K, value V) bool) bool {
	if t == nil {
		return true
	}

	if ok := t.left._range(f); !ok {
		return false
	}
	if ok := f(t.key, t.value); !ok {
		return false
	}
	if ok := t.right._range(f); !ok {
		return false
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

func (t *Tree[K, V]) Insert(key K, value V) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	child := &node[K, V]{
		key:   key,
		value: value,
	}

	return insert(&t.root, child)
}

func (t *Tree[K, V]) Get(key K) (V, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	n := find(&t.root, key)
	if n == nil {
		var v V
		return v, ErrNotFound
	}

	return n.value, nil
}

func (t *Tree[K, V]) Remove(key K) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.root == nil {
		return ErrNotFound
	}

	if t.root._height() == 1 && t.root.key == key {
		t.root = nil
		return nil
	}

	err := remove(&t.root, key)
	if err != nil {
		return err
	}
	return nil
}

func (t *Tree[K, V]) Range(f func(key K, value V) bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	t.root._range(f)
}
