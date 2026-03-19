package notify

import "sync"

type Broadcaster struct {
	mutex   sync.RWMutex
	channel chan struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		channel: make(chan struct{}),
	}
}

func (n *Broadcaster) Receive() <-chan struct{} {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.channel
}

func (n *Broadcaster) Notify() {
	newChannel := make(chan struct{})
	n.mutex.Lock()
	channelToClose := n.channel
	n.channel = newChannel
	n.mutex.Unlock()
	close(channelToClose)
}
