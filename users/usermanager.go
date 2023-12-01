package users

import "scratchdb/servers"

type UserManager interface {
	AddUser(name string) error
	GetDBManager() servers.DatabaseServerManager
}
