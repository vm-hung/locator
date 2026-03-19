package types

import "fmt"

type Endpoint struct {
	Host string
	Port int
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}
