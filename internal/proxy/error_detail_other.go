//go:build !windows

package proxy

import "syscall"

func normalizePlatformErrno(syscall.Errno) (string, bool) {
	return "", false
}
