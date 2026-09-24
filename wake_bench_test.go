package morsel

import (
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkWakeChannel(b *testing.B) {
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			<-ch
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- struct{}{}
	}
	wg.Wait()
}

func BenchmarkWakeChannelCap1(b *testing.B) {
	ch := make(chan struct{}, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			select {
			case <-ch:
			case <-stop:
				return
			}
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- struct{}{}
	}
	wg.Wait()
	close(stop)
}

func BenchmarkWakeCond(b *testing.B) {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	ready := 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			mu.Lock()
			for ready == 0 {
				cond.Wait()
			}
			ready--
			mu.Unlock()
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		ready++
		cond.Signal()
		mu.Unlock()
	}
	wg.Wait()
}

func BenchmarkWakeSpin(b *testing.B) {
	var flag atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			for flag.Load() != 1 {
			}
			flag.Store(0)
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for flag.Load() != 0 {
		}
		flag.Store(1)
	}
	wg.Wait()
}
