package schedule

import (
	"context"
	"log"
	"sync"
)

type jobQueue struct {
	mutex sync.Mutex

	scheduled int
	finished  int
	pendings  []Job

	ctx    context.Context
	cancel context.CancelFunc

	finish *sync.Cond
	resume chan struct{}
	done   chan struct{}
}

func NewJobQueue() Scheduler {
	queue := &jobQueue{
		resume: make(chan struct{}, 1),
		done:   make(chan struct{}, 1),
	}
	queue.finish = sync.NewCond(&queue.mutex)
	queue.ctx, queue.cancel = context.WithCancel(context.Background())
	go queue.run()
	return queue
}

func (q *jobQueue) Schedule(j Job) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.cancel == nil {
		panic("schedule: schedule to stopped scheduler")
	}

	if len(q.pendings) == 0 {
		select {
		case q.resume <- struct{}{}:
		default:
		}
	}
	q.pendings = append(q.pendings, j)
}

func (q *jobQueue) Pending() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return len(q.pendings)
}

func (q *jobQueue) Scheduled() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.scheduled
}

func (q *jobQueue) Finished() int {
	q.finish.L.Lock()
	defer q.finish.L.Unlock()
	return q.finished
}

func (q *jobQueue) WaitFinish(n int) {
	q.finish.L.Lock()
	for q.finished < n || len(q.pendings) != 0 {
		q.finish.Wait()
	}
	q.finish.L.Unlock()
}

func (q *jobQueue) Stop() {
	q.mutex.Lock()
	q.cancel()
	q.cancel = nil
	q.mutex.Unlock()
	<-q.done
}

func (q *jobQueue) run() {
	defer func() {
		close(q.done)
		close(q.resume)
	}()

	for {
		var todo Job
		q.mutex.Lock()
		if len(q.pendings) != 0 {
			q.scheduled++
			todo = q.pendings[0]
		}
		q.mutex.Unlock()
		if todo == nil {
			select {
			case <-q.resume:
			case <-q.ctx.Done():
				q.mutex.Lock()
				pendings := q.pendings
				q.pendings = nil
				q.mutex.Unlock()
				for _, todo := range pendings {
					q.executeJob(todo, true)
				}
				return
			}
		} else {
			q.executeJob(todo, false)
		}
	}
}

func (q *jobQueue) executeJob(todo Job, updatedFinishedStats bool) {
	defer func() {
		if !updatedFinishedStats {
			q.finish.L.Lock()
			q.finished++
			q.pendings = q.pendings[1:]
			q.finish.Broadcast()
			q.finish.L.Unlock()
		}
		if err := recover(); err != nil {
			log.Fatalf("execute job failed: %v", err)
		}
	}()

	todo.Do(q.ctx)
}
