package notify

type Completion struct {
	channel chan struct{}
	err     error
}

func NewCompletion() *Completion {
	return &Completion{
		channel: make(chan struct{}),
	}
}

func (n *Completion) Notify(err error) {
	n.err = err
	close(n.channel)
}

func (n *Completion) Signal() <-chan struct{} {
	return n.channel
}

func (n *Completion) Error() error {
	return n.err
}
