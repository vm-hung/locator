package service

import (
	"time"

	"google.golang.org/grpc"
)

type MemberService struct {
}

func NewMemberService(conn *grpc.ClientConn, timeout time.Duration) *MemberService {
	return &MemberService{}
}
