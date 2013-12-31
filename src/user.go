package eaglemq

const (
	EG_USER_ALL_PERM   = 0x3F
	EG_USER_SUPER_PERM = 0x7F

	EG_USER_QUEUE_PERM      = 0
	EG_USER_ROUTE_PERM      = 1
	EG_USER_CHANNEL_PERM    = 2
	EG_USER_RESV3_PERM      = 3
	EG_USER_RESV4_PERM      = 4
	EG_USER_ADMIN_PERM      = 5
	EG_USER_NOT_CHANGE_PERM = 6

	EG_USER_QUEUE_CREATE_PERM      = 20
	EG_USER_QUEUE_DECLARE_PERM     = 21
	EG_USER_QUEUE_EXIST_PERM       = 22
	EG_USER_QUEUE_LIST_PERM        = 23
	EG_USER_QUEUE_RENAME_PERM      = 24
	EG_USER_QUEUE_SIZE_PERM        = 25
	EG_USER_QUEUE_PUSH_PERM        = 26
	EG_USER_QUEUE_GET_PERM         = 27
	EG_USER_QUEUE_POP_PERM         = 28
	EG_USER_QUEUE_CONFIRM_PERM     = 29
	EG_USER_QUEUE_SUBSCRIBE_PERM   = 30
	EG_USER_QUEUE_UNSUBSCRIBE_PERM = 31
	EG_USER_QUEUE_PURGE_PERM       = 32
	EG_USER_QUEUE_DELETE_PERM      = 33

	EG_USER_ROUTE_CREATE_PERM = 34
	EG_USER_ROUTE_EXIST_PERM  = 35
	EG_USER_ROUTE_LIST_PERM   = 36
	EG_USER_ROUTE_KEYS_PERM   = 37
	EG_USER_ROUTE_RENAME_PERM = 38
	EG_USER_ROUTE_BIND_PERM   = 39
	EG_USER_ROUTE_UNBIND_PERM = 40
	EG_USER_ROUTE_PUSH_PERM   = 41
	EG_USER_ROUTE_DELETE_PERM = 42

	EG_USER_CHANNEL_CREATE_PERM       = 43
	EG_USER_CHANNEL_EXIST_PERM        = 44
	EG_USER_CHANNEL_LIST_PERM         = 45
	EG_USER_CHANNEL_RENAME_PERM       = 46
	EG_USER_CHANNEL_PUBLISH_PERM      = 47
	EG_USER_CHANNEL_SUBSCRIBE_PERM    = 48
	EG_USER_CHANNEL_PSUBSCRIBE_PERM   = 49
	EG_USER_CHANNEL_UNSUBSCRIBE_PERM  = 50
	EG_USER_CHANNEL_PUNSUBSCRIBE_PERM = 51
	EG_USER_CHANNEL_DELETE_PERM       = 52
)

type EagleUser struct {
	name     string
	password string
	perm     uint64
}

// ALL FUNCTIONS PUBLIC

func create_user(name, password string, perm uint64) *EagleUser {
	user = &EagleUser{
		name:     name,
		password: password,
		perm:     perm,
	}
	return user
}

func delete_user(user *EagleUser) {
	xfree(user)
}

func find_user(list *List, name string, password *string) *EagleUser {
	var iterator ListIterator

	list_rewind(list, &iterator)
	for node = list_next_node(&iterator); node != nil; node = list_next_node(&iterator) {
		user := EG_LIST_NODE_VALUE(node)
		if password != nil {
			if user.name == name && user.password == password {
				return user
			}
		} else {
			if user.name == name {
				return user
			}
		}
	}

	return nil
}

func rename_user(user *EagleUser, name string) {
	user.name = name
}

func set_user_perm(user *EagleUser, perm uint64) {
	user.perm = perm
}

func get_user_perm(user *EagleUser) uint64 {
	return user.perm
}

func free_user_list_handler(ptr interface{}) {
	delete_user(*EagleUser(ptr))
}
