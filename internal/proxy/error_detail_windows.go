//go:build windows

package proxy

import "syscall"

func normalizePlatformErrno(errno syscall.Errno) (string, bool) {
	switch errno {
	case syscall.WSAECONNRESET:
		return "ECONNRESET", true
	case syscall.WSAECONNABORTED:
		return "ECONNABORTED", true
	default:
		return "", false
	}
}
