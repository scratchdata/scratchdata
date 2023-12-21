package util

import "golang.org/x/sys/unix"

func FreeDiskSpace(path string) uint64 {
	if path == "" {
		path = "/"
	}

	var stat unix.Statfs_t
	unix.Statfs(path, &stat)
	currentFreeSpace := stat.Bavail * uint64(stat.Bsize)
	return currentFreeSpace

}
