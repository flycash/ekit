// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"context"
	"sync"
)

// ConcurrentBlockingQueue 有界并发阻塞队列
type ConcurrentBlockingQueue[T any] struct {
	data  []T
	mutex sync.RWMutex

	// 队头元素下标
	head int
	// 队尾元素下标
	tail int
	// 包含多少个元素
	count int

	enqueueSignal chan struct{}
	dequeueSignal chan struct{}

	notEmpty *sync.Cond
	notFull  *sync.Cond

	zero T
}

// NewConcurrentBlockingQueue 创建一个有界阻塞队列
// 容量会在最开始的时候就初始化好
// capacity 必须为正数
func NewConcurrentBlockingQueue[T any](capacity int) *ConcurrentBlockingQueue[T] {
	return &ConcurrentBlockingQueue[T]{
		data:          make([]T, capacity),
		enqueueSignal: make(chan struct{}),
		dequeueSignal: make(chan struct{}),
	}
}

func (c *ConcurrentBlockingQueue[T]) Enqueue(ctx context.Context, t T) error {
	for {
		c.mutex.Lock()
		full := c.count == len(c.data)

		if full {
			// 要开始睡眠了
			c.mutex.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.dequeueSignal:

			}
		} else {
			c.data[c.tail] = t
			c.tail++
			// c.tail 已经是最后一个了，重置下标
			if c.tail == cap(c.data) {
				c.tail = 0
			}
			go func() {
				c.enqueueSignal <- struct{}{}
			}()
			c.mutex.Unlock()
			return nil
		}
	}
}

func (c *ConcurrentBlockingQueue[T]) EnqueueV2(ctx context.Context, t T) chan error {
	signal := make(chan error, 1)
	defer close(signal)
	err := c.enqueue(t)
	signal <- err
	return signal
}

func (c *ConcurrentBlockingQueue[T]) EnqueueV1(ctx context.Context, t T) error {
	signal := make(chan error)
	go func() {
		err := c.enqueue(t)
		select {
		case signal <- err:
		default:
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-signal:
		return err
	}
}

func (c *ConcurrentBlockingQueue[T]) enqueue(t T) error {
	for {
		c.mutex.Lock()
		for c.count == len(c.data) {
			c.notFull.Wait()
		}

		c.data[c.tail] = t
		c.tail++
		// c.tail 已经是最后一个了，重置下标
		if c.tail == cap(c.data) {
			c.tail = 0
		}
		c.notEmpty.Signal()
		c.mutex.Unlock()
		return nil
	}
}

func (c *ConcurrentBlockingQueue[T]) Dequeue(ctx context.Context) (T, error) {
	for {
		c.mutex.Lock()
		if c.count == 0 {
			c.mutex.Unlock()
			select {
			case <-ctx.Done():
				var t T
				return t, ctx.Err()
			case <-c.enqueueSignal:

			}
		} else {
			val := c.data[c.head]
			// 为了释放内存，GC
			c.data[c.head] = c.zero
			c.count--
			c.head++
			// 重置下标
			if c.head == cap(c.data) {
				c.head = 0
			}

			go func() {
				c.dequeueSignal <- struct{}{}
			}()
			c.mutex.Unlock()
			return val, nil
		}
	}
}
