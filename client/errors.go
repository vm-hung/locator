package client

import "errors"

var (
	ErrInvalidAddress    = errors.New("invalid address")
	ErrInvalidPortNumber = errors.New("invalid port number")
	ErrInvalidTimeout    = errors.New("invalid timeout")
	ErrUnimplementedYet  = errors.New("unimplemented yet")
)
