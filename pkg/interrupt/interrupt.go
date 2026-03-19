package interrupt

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Finalizer func(context.Context)

type InterruptorCallback struct {
	OnShutdownSignal   func()
	OnContextCancelled func()
	OnNoCleanupTasks   func()
	OnFinalizeStart    func()
	OnTaskPanic        func(any)
	OnTaskCompleted    func()
	OnCleanupTimeout   func()
}

type Interruptor struct {
	finalizers []Finalizer
	timeout    time.Duration
	context    context.Context
	cancelFunc context.CancelFunc
	cancelled  bool
	mutex      sync.Mutex
	callback   *InterruptorCallback
}

type Option func(*Interruptor)

func WithTimeout(timeout time.Duration) Option {
	return func(i *Interruptor) {
		i.timeout = timeout
	}
}

func WithFinalizers(finalizers ...Finalizer) Option {
	return func(i *Interruptor) {
		i.finalizers = append(i.finalizers, finalizers...)
	}
}

func WithCallback(callback *InterruptorCallback) Option {
	return func(i *Interruptor) {
		i.callback = callback
	}
}

func New(options ...Option) *Interruptor {
	parent := context.Background()
	ctx, cancel := context.WithCancel(parent)
	interruptor := &Interruptor{
		timeout:    10 * time.Second,
		context:    ctx,
		cancelFunc: cancel,
		cancelled:  false,
	}
	for _, option := range options {
		option(interruptor)
	}
	return interruptor
}

func (i *Interruptor) Register(finalizer Finalizer) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.finalizers = append(i.finalizers, finalizer)
}

func (i *Interruptor) Listen() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	select {
	case <-quit:
		fmt.Print("\r\033[K")
		if i.callback != nil && i.callback.OnShutdownSignal != nil {
			i.callback.OnShutdownSignal()
		}
	case <-i.context.Done():
		if i.callback != nil && i.callback.OnContextCancelled != nil {
			i.callback.OnContextCancelled()
		}
	}

	i.mutex.Lock()
	finalizers := append([]Finalizer{}, i.finalizers...)
	i.mutex.Unlock()

	remaining := len(finalizers)
	if remaining == 0 {
		if i.callback != nil && i.callback.OnNoCleanupTasks != nil {
			i.callback.OnNoCleanupTasks()
		}
		return
	}

	done := make(chan struct{}, remaining)

	if i.callback != nil && i.callback.OnFinalizeStart != nil {
		i.callback.OnFinalizeStart()
	}

	for idx := len(finalizers) - 1; idx >= 0; idx-- {
		go func(finalizer Finalizer, done chan struct{}) {
			defer func() {
				if r := recover(); r != nil {
					if i.callback != nil && i.callback.OnTaskPanic != nil {
						i.callback.OnTaskPanic(r)
					}
				}
			}()
			finalizer(i.context)
			done <- struct{}{}
		}(finalizers[idx], done)
	}

	timer := time.NewTimer(i.timeout)
	defer timer.Stop()

	for remaining > 0 {
		select {
		case <-timer.C:
			if i.callback != nil && i.callback.OnCleanupTimeout != nil {
				i.callback.OnCleanupTimeout()
			}
			os.Exit(1)
		case <-done:
			remaining--
		}
	}

	if i.callback != nil && i.callback.OnTaskCompleted != nil {
		i.callback.OnTaskCompleted()
	}
}

func (i *Interruptor) Cancel() {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if !i.cancelled {
		i.cancelFunc()
	}
}
