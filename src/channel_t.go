package eaglemq

type Channel_t struct {
	name        string
	flags       uint32
	auto_delete int
	round_robin int
	topics      *Keylist
	patterns    *Keylist
}

// TODO(markcol): all of these functions should be converted to be object
// functions of type Channel_t.

// TODO(markcol): review and update all doc comments so that API documentation
// is correct.

// TODO(markcol): add example and overview documentation comments.

// create_channel_t creates a channel with the given name and flag
// values. Returns a new channel with no topics or patterns. Autodelete and
// Round Robin flags are set based on the global default values.
//
// TODO(markcol): change name NewChannel to match Go idioms.
func create_channel_t(name string, flags uint32) *Channel_t {
	channel := &Channel_t{
		name:  name,
		flags: flags,
	}

	if BIT_CHECK(channel.flags, EG_CHANNEL_AUTODELETE_FLAG) {
		channel.auto_delete = 1
	}

	if BIT_CHECK(channel.flags, EG_CHANNEL_ROUND_ROBIN_FLAG) {
		channel.round_robin = 1
	}

	channel.topics = keylist_create()
	channel.patterns = keylist_create()

	EG_KEYLIST_SET_FREE_METHOD(channel.topics, free_channel_keylist_handler)
	EG_KEYLIST_SET_MATCH_METHOD(channel.topics, match_channel_keylist_handler)

	EG_KEYLIST_SET_FREE_METHOD(channel.patterns, free_channel_keylist_handler)
	EG_KEYLIST_SET_MATCH_METHOD(channel.patterns, match_channel_keylist_handler)

	return channel
}

// delete_channel_t deletes the channel
func delete_channel_t(channel *Channel_t) {
	eject_clients_channel_t(channel)

	keylist_release(channel.topics)
	keylist_release(channel.patterns)
}

// publish_message_channel_t
func publish_message_channel_t(channel *Channel_t, topic string, msg *Object) {
	var (
		keylist_iterator KeylistIterator
		list_iterator    ListIterator
	)

	keylist_node := keylist_get_value(channel.topics, topic)
	if keylist_node {
		list := EG_KEYLIST_NODE_VALUE(keylist_node)

		if channel.round_robin {
			list_rotate(list)

			client := EG_LIST_NODE_VALUE(EG_LIST_FIRST(list))
			channel_client_event_message(client, channel, topic, msg)
		} else {
			list_rewind(list, &list_iterator)
			for list_node := list_next_node(&list_iterator); list_node != nil; list_node = list_next_node(&list_iterator) {
				client := EG_LIST_NODE_VALUE(list_node)
				channel_client_event_message(client, channel, topic, msg)
			}
		}
	}

	if EG_KEYLIST_LENGTH(channel.patterns) {
		keylist_rewind(channel.patterns, &keylist_iterator)
		for keylist_node = keylist_next_node(&keylist_iterator); keylist_node != nil; keylist_node = keylist_next_node(&keylist_iterator) {
			pattern := EG_KEYLIST_NODE_KEY(keylist_node)

			if pattern_match(topic, pattern, 0) {
				list := EG_KEYLIST_NODE_VALUE(keylist_node)

				list_rewind(list, &list_iterator)
				for list_node = list_next_node(&list_iterator); list_node != nil; list_node = list_next_node(&list_terator) {
					client := EG_LIST_NODE_VALUE(list_node)
					channel_client_event_pattern_message(client, channel, topic, pattern, msg)
				}
			}
		}
	}
}

// find_channel_t returns the first channel in the list that matches name.
func find_channel_t(list *List, name string) *Channel_t {
	var ListIterator iterator

	list_rewind(list, &iterator)
	for node = list_next_node(&iterator); node != nil; node = list_next_node(&iterator) {
		channel := EG_LIST_NODE_VALUE(node)
		if channel.name == name {
			return channel
		}
	}

	return nil
}

// rename_channel_t changes the name of the channel.
func rename_channel_t(channel *Channel_t, name string) {
	channel.name = name
}

// subscribe_channel_t subscribes the client to the channel with the given
// topic string(?)
func subscribe_channel_t(channel *Channel_t, client *EagleClient, topic string) {
	var (
		list      *List
		subscribe = 0
	)

	node := keylist_get_value(channel.topics, topic)
	if node == nil {
		list = list_create()
		keylist_set_value(channel.topics, xstrdup(topic), list)
		node = EG_KEYLIST_LAST(channel.topics)
		subscribe = 1
	} else {
		list = EG_KEYLIST_NODE_VALUE(node)
		if !list_search_node(list, client) {
			subscribe = 1
		}
	}

	if subscribe {
		list_add_value_tail(list, client)
		add_keylist_channel_t(client.subscribed_topics, channel, node)
	}
}

// punsubscribe_channel_t removes the client's subscript to channels that
// match the regular expression provided in pattern.
func psubscribe_channel_t(channel *Channel_t, client *EagleClient, pattern string) {
	var (
		subscribe = false
		list      *List
	)

	node := keylist_get_value(channel.patterns, pattern)
	if !node {
		list = list_create()
		keylist_set_value(channel.patterns, pattern, list)
		node = EG_KEYLIST_LAST(channel.patterns)
		subscribe = true
	} else {
		list = EG_KEYLIST_NODE_VALUE(node)
		if !list_search_node(list, client) {
			subscribe = true
		}
	}

	if subscribe {
		list_add_value_tail(list, client)
		add_keylist_channel_t(client.subscribed_patterns, channel, node)
	}
}

// unsubscribe_channel_t removes the client's subscription to the given channel.
func unsubscribe_channel_t(channel *Channel_t, client *EagleClient, topic string) int {
	node := keylist_get_value(channel.topics, topic)
	if node == nil {
		return EG_STATUS_ERR
	}

	list := EG_KEYLIST_NODE_VALUE(node)

	list_delete_value(list, client)
	remove_keylist_channel_t(client.subscribed_topics, channel, node)

	if !EG_LIST_LENGTH(list) {
		keylist_delete_node(channel.topics, node)
	}

	if channel.auto_delete {
		if EG_KEYLIST_LENGTH(channel.topics) == 0 && EG_KEYLIST_LENGTH(channel.patterns) == 0 {
			list_delete_value(server.channels, channel)
		}
	}

	return EG_STATUS_OK
}

// punsubscribe_channel_t
func punsubscribe_channel_t(channel *Channel_t, client *EagleClient, pattern string) int {
	node := keylist_get_value(channel.patterns, pattern)
	if node == nil {
		return EG_STATUS_ERR
	}

	list := EG_KEYLIST_NODE_VALUE(node)

	list_delete_value(list, client)
	remove_keylist_channel_t(client.subscribed_patterns, channel, node)

	if !EG_LIST_LENGTH(list) {
		keylist_delete_node(channel.patterns, node)
	}

	if channel.auto_delete {
		if EG_KEYLIST_LENGTH(channel.topics) == 0 &&
			EG_KEYLIST_LENGTH(channel.patterns) == 0 {
			list_delete_value(server.channels, channel)
		}
	}

	return EG_STATUS_OK
}

// free_channel_list_handler
func free_channel_list_handler(ptr interface{}) {
	delete_channel_t(ptr)
}

// *private* functions below this point

// eject_clients_channel_t
func eject_clients_channel_t(channel *Channel_t) {
	var (
		keylist_iterator KeylistIterator
		list_iterator    ListIterator
	)

	keylist_rewind(channel.topics, &keylist_iterator)
	for keylist_node := keylist_next_node(&keylist_iterator); keylist_node != nil; keylist_node = keylist_next_node(&keylist_iterator) {
		list := EG_KEYLIST_NODE_VALUE(keylist_node)

		list_rewind(list, &list_iterator)
		for list_node := list_next_node(&list_iterator); list_node != nil; list_node = list_next_node(&list_iterator) {
			client := EG_LIST_NODE_VALUE(list_node)
			unsubscribe_channel_t(channel, client, EG_KEYLIST_NODE_KEY(keylist_node))
		}
	}

	keylist_rewind(channel.patterns, &keylist_iterator)
	for keylist_node = keylist_next_node(&keylist_iterator); keylist_node != nil; keylist_node = keylist_next_node(&keylist_iterator) {
		list := EG_KEYLIST_NODE_VALUE(keylist_node)

		list_rewind(list, &list_iterator)
		for list_node := list_next_node(&list_iterator); list_node != nil; list_node = list_next_node(&list_iterator) {
			client := EG_LIST_NODE_VALUE(list_node)
			punsubscribe_channel_t(channel, client, EG_KEYLIST_NODE_KEY(keylist_node))
		}
	}
}

// free_channel_keylist_handler hooks the release of the channel and does the
// appropriate cleanup. Should not be needed in Go.
func free_channel_keylist_handler(key, value interface{}) {
	list_release(value)
}

// match_channel_keylist_handler returns true if the two keys match.
// May need to convert parameters to interface{} and use string assertions.
func match_channel_keylist_handler(key1, key2 string) int {
	return key1 == key2
}

// remove_keylist_channel_t
func remove_keylist_channel_t(keylist *Keylist, channel *Channel_t, keylist_node *KeylistNode) int {
	node := keylist_get_value(keylist, channel)
	if !node {
		return EG_STATUS_ERR
	}

	list := EG_KEYLIST_NODE_VALUE(node)
	list_delete_value(list, keylist_node)

	if !EG_LIST_LENGTH(list) {
		list_release(list)
		keylist_delete_node(keylist, node)
	}

	return EG_STATUS_OK
}

// add_keylist_channel_t
func add_keylist_channel_t(Keylist *keylist, Channel_t *channel, KeylistNode *keylist_node) {
	var (
		list *List
		add  int
	)

	node := keylist_get_value(keylist, channel)
	if !node {
		list = list_create()
		keylist_set_value(keylist, channel, list)
		add = 1
	} else {
		list = EG_KEYLIST_NODE_VALUE(node)
		if !list_search_node(list, keylist_node) {
			add = 1
		}
	}

	if add {
		list_add_value_tail(list, keylist_node)
	}
}
