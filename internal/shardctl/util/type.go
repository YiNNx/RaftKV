package util

// Set

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return map[T]struct{}{}
}

func (s Set[T]) Add(element T) {
	s[element] = struct{}{}
}

func (s Set[T]) Contains(element T) bool {
	_, ok := s[element]
	return ok
}

func (s Set[T]) Remove() T {
	var k T
	for k = range s {
		break
	}
	delete(s, k)
	return k
}

// Priority Queue

type Item struct {
	Value    int
	Priority int // The priority of the item in the queue.
	Index    int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	items []*Item
	max   bool
}

func NewPQ(max bool) PriorityQueue {
	return PriorityQueue{
		items: make([]*Item, 0),
		max:   max,
	}
}

func (pq PriorityQueue) Len() int { return len(pq.items) }

func (pq PriorityQueue) Less(i, j int) bool {
	if pq.items[i].Priority == pq.items[j].Priority {
		return pq.max == (pq.items[i].Value > pq.items[j].Value)
	}
	return pq.max == (pq.items[i].Priority > pq.items[j].Priority)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(pq.items)
	item := x.(*Item)
	item.Index = n
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old.items)
	item := old.items[n-1]
	old.items[n-1] = nil // don't stop the GC from reclaiming the item eventually
	item.Index = -1      // for safety
	pq.items = old.items[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peak() *Item {
	if len(pq.items) == 0 {
		return nil
	}
	return pq.items[0]
}
