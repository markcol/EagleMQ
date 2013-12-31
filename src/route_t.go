package eaglemq

type Route_t struct {
	name string
	flags uint32
	auto_delete int
	round_robin int
	keys *Keylist
}

func create_route_t(name string, flags uint32) *Route_t {
	route := &Route_t{
		name: name,
		flags: flags,
	}

	if (BIT_CHECK(route.flags, EG_ROUTE_AUTODELETE_FLAG)) {
		route.auto_delete = 1;
	}

	if (BIT_CHECK(route.flags, EG_ROUTE_ROUND_ROBIN_FLAG)) {
		route.round_robin = 1;
	}

	route.keys = keylist_create();

	EG_KEYLIST_SET_FREE_METHOD(route.keys, free_route_keylist_handler);
	EG_KEYLIST_SET_MATCH_METHOD(route.keys, match_route_keylist_handler);

	return route;
}

func delete_route_t(route *Route_t) {
	unbind_queues_route_t(route);
	keylist_release(route.keys);
	xfree(route);
}

func push_message_route_t(route *Route_t, key string, msg *Object, expiration uint32) int {
	var (
		status = EG_STATUS_OK;
		list_iterator ListIterator
	)

	keylist_node := keylist_get_value(route.keys, key);
	if (!keylist_node) {
		return EG_STATUS_ERR;
	}

	list := EG_KEYLIST_NODE_VALUE(keylist_node);

	if (route.round_robin)	{
		list_rotate(list);

		queue_t := EG_LIST_NODE_VALUE(EG_LIST_FIRST(list));

		if (push_message_queue_t(queue_t, msg, expiration) != EG_STATUS_OK){
			status = EG_STATUS_ERR;
		}

		increment_references_count(msg);
	} else {
		list_rewind(list, &list_iterator);
		for list_node := list_next_node(&list_iterator); list_node != nil; list_node := list_next_node(&list_iterator) {
			queue_t := EG_LIST_NODE_VALUE(list_node);

			if (push_message_queue_t(queue_t, msg, expiration) != EG_STATUS_OK){
				status = EG_STATUS_ERR;
			}

			increment_references_count(msg);
		}
	}

	return status;
}

func bind_route_t(route *Route_t, queue_t *Queue_t, key string) {
	var (
		link = 0;
		list *List
	)

	node := keylist_get_value(route.keys, key);
	if (!node)	{
		list = list_create();
		keylist_set_value(route.keys, xstrdup(key), list);
		link = 1;
	}	else	{
		list = EG_KEYLIST_NODE_VALUE(node);
		if (!list_search_node(list, queue_t)) {
			link = 1;
		}
	}

	if (link)	{
		list_add_value_tail(list, queue_t);
		link_queue_route_t(queue_t, route, key);
	}
}

func unbind_route_t(route *Route_t, queue_t *Queue_t, key string) int {
	node := keylist_get_value(route.keys, key);
	if (!node) {
		return EG_STATUS_ERR;
	}

	list := EG_KEYLIST_NODE_VALUE(node);
	list_delete_value(list, queue_t);

	if (!EG_LIST_LENGTH(list)) {
		keylist_delete_node(route.keys, node);
	}

	unlink_queue_route_t(queue_t, route, key);

	if (route.auto_delete)	{
		if (EG_KEYLIST_LENGTH(route.keys) == 0) {
			list_delete_value(server.routes, route);
		}
	}

	return EG_STATUS_OK;
}

func find_route_t(list *List, name string) *Route_t {
	var iterator ListIterator

	list_rewind(list, &iterator);
	for node := list_next_node(&iterator); node != nil; node = list_next_node(&iterator) {
		route := EG_LIST_NODE_VALUE(node);
		if route.name == name {
			return route;
		}
	}

	return nil;
}

func rename_route_t(route *Route_t, name string) {
	route.name = name
}

func get_queue_number_route_t(route *Route_t) uint32 {
	var (
		iterator KeylistIterator
		queues = 0;
	)

	keylist_rewind(route.keys, &iterator);
	for node := keylist_next_node(&iterator); node != nil; node = keylist_next_node(&iterator) {
		list := EG_KEYLIST_NODE_VALUE(node);
		queues += EG_LIST_LENGTH(list);
	}

	return queues;
}

func free_route_list_handler(ptr interface{}) {
	delete_route_t(Route_t*(ptr));
}

// *private* functions only beyond this point

static void free_route_keylist_handler(void *key, void *value)
{
	xfree(key);
	list_release(value);
}

static int match_route_keylist_handler(void *key1, void *key2)
{
	return !strcmp(key1, key2);
}

static inline void unbind_queues_key_route_t(Route_t *route, List *queues, const char *key)
{
	ListIterator iterator;
	ListNode *node;
	Queue_t *queue_t;

	list_rewind(queues, &iterator);
	while ((node = list_next_node(&iterator)) != nil)
	{
		queue_t = EG_LIST_NODE_VALUE(node);

		unlink_queue_route_t(queue_t, route, key);
	}
}

static void unbind_queues_route_t(Route_t *route)
{
	KeylistIterator iterator;
	KeylistNode *node;

	keylist_rewind(route.keys, &iterator);
	while ((node = keylist_next_node(&iterator)) != nil)
	{
		unbind_queues_key_route_t(route, EG_KEYLIST_NODE_VALUE(node), EG_KEYLIST_NODE_KEY(node));
	}
}
