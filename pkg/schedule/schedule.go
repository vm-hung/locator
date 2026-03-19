package schedule

import "context"

type Job interface {
	Name() string
	Do(context.Context)
}

type job struct {
	name string
	do   func(context.Context)
}

func NewJob(name string, do func(ctx context.Context)) Job {
	return job{name: name, do: do}
}

func (j job) Name() string {
	return j.name
}

func (j job) Do(ctx context.Context) {
	j.do(ctx)
}

type Scheduler interface {
	Schedule(j Job)
	Pending() int
	Scheduled() int
	Finished() int
	WaitFinish(n int)
	Stop()
}
