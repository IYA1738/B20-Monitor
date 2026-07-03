package graceful

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type CleanupFunc func(ctx context.Context) error

type cleanupItem struct {
	name string
	fn   CleanupFunc
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	running atomic.Bool

	cleanupMu sync.Mutex
	cleanups  []cleanupItem // 上面的互斥锁是为了给go rountine读写这个的时候加锁

	shutdownOnce sync.Once // 确保只做一次
	exitDone     chan struct{}

	shutdownTimeout time.Duration
	cleanupTimeout  time.Duration
}

func New() *Manager {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	m := &Manager{
		ctx:             ctx,
		cancel:          cancel,
		exitDone:        make(chan struct{}),
		shutdownTimeout: 30 * time.Second,
		cleanupTimeout:  30 * time.Second,
	}

	m.running.Store(true)

	return m
}

func (m *Manager) Context() context.Context {
	return m.ctx
}

func (m *Manager) Done() <-chan struct{} {
	return m.ctx.Done()
}

func (m *Manager) ExitDone() <-chan struct{} {
	return m.exitDone
}

func (m *Manager) Normal() bool {
	return m.running.Load()
}

// Manager启动一个Go Routine
func (m *Manager) Go(name string, fn func(ctx context.Context) error) {
	if !m.Normal() {
		log.Printf("[graceful] skip starting service=%s because manager is stopping", name)
		return
	}

	m.wg.Add(1)

	go func() {

		defer func() {
			// Go的压缩语法 r:=recover()初始化r 然后再去看r
			// 等价于 r := recover() if r != nil
			if r := recover(); r != nil { // recover()就像try catch一样去捕获panic
				log.Printf("[graceful] panic recovered service=%s panic=%v\n%s", name, r, debug.Stack())
				m.Stop()
			}
			m.wg.Done()
		}()

		if err := fn(m.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("[graceful] service=%s deadline exceed:%v", name, err)
				m.Stop()
				return
			}
			log.Printf("[graceful] service=%s exited with error: %v", name, err)
			m.Stop()
		}
	}()
}

func (m *Manager) RegisterCleanup(name string, fn CleanupFunc) {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	m.cleanups = append(m.cleanups, cleanupItem{
		name: name,
		fn:   fn,
	})
}

func (m *Manager) Stop() {
	if !m.running.CompareAndSwap(true, false) {
		return
	}

	log.Println("[graceful] stop requested")
	m.cancel()
}

func (m *Manager) Shutdown() {
	m.shutdownOnce.Do(
		func() {
			m.Stop()

			log.Println("[graceful] shutdown starting")

			m.waitGoroutine()
			m.runCleanups()

			close(m.exitDone)
			log.Println("[graceful] shutdown done")
		},
	)
}

func (m *Manager) Wait() {
	<-m.ctx.Done()
	m.Shutdown()
}

func (m *Manager) waitGoroutine() {
	done := make(chan struct{})

	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[graceful] all goroutines stopped")
	case <-time.After(m.shutdownTimeout):
		log.Printf("[graceful] wait goroutines timeout after %s", m.shutdownTimeout)
	}
}

func (m *Manager) runCleanups() {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), m.cleanupTimeout)
	defer cancel()

	m.cleanupMu.Lock()
	cleanups := make([]cleanupItem, len(m.cleanups))
	copy(cleanups, m.cleanups)
	m.cleanupMu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		item := cleanups[i]

		log.Printf("[graceful] cleanup starting name = %s", item.name)

		if err := item.fn(cleanupCtx); err != nil {
			log.Printf("[graceful] cleanup failed name=%s err=%v", item.name, err)
			continue
		}

		log.Printf("[graceful] cleanup done name = %s", item.name)
	}
}
