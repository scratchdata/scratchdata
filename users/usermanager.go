package users

import servers "scratchdb/servers_old"

type UserManager interface {
	AddUser(name string) error
	GetDBManager() servers.ClickhouseManager
}
