package wait

import "sync"

type Deadline interface {
	Wait(deadline uint64) <-chan struct{}
	Trigger(deadline uint64)
}

var closec chan struct{}

func init() { closec = make(chan struct{}); close(closec) }

type timeList struct {
	mutex               sync.Mutex
	lastTriggerDeadline uint64
	channels            map[uint64]chan struct{}
}

func NewDeadline() Deadline {
	return &timeList{channels: make(map[uint64]chan struct{})}
}

func (tl *timeList) Wait(deadline uint64) <-chan struct{} {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	if tl.lastTriggerDeadline >= deadline {
		return closec
	}
	ch := tl.channels[deadline]
	if ch == nil {
		ch = make(chan struct{})
		tl.channels[deadline] = ch
	}
	return ch
}

func (tl *timeList) Trigger(deadline uint64) {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	tl.lastTriggerDeadline = deadline
	for t, ch := range tl.channels {
		if t <= deadline {
			delete(tl.channels, t)
			close(ch)
		}
	}
}
