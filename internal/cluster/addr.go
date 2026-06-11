package cluster

import (
	"net"
	"strconv"
)

func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return host, port, nil
}
