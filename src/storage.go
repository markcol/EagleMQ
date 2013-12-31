package eaglemq

const (
	EG_STORAGE_VERSION =1

	EG_STORAGE_TYPE_USER =0x1
	EG_STORAGE_TYPE_QUEUE =0x2
	EG_STORAGE_TYPE_MESSAGE =0x3
	EG_STORAGE_TYPE_ROUTE =0x4
	EG_STORAGE_TYPE_ROUTE_KEY =0x5
	EG_STORAGE_TYPE_CHANNEL =0x6

	EG_STORAGE_EOF = 0xFF
)


func storage_load(filename string) int {
	FILE *fp;
	int type;
	int state = 1;

	fp = fopen(filename, "r");
	if (!fp) {
		warning("Failed opening storage for loading: %s",  strerror(errno));
		return EG_STATUS_ERR;
	}

	if (storage_read_magic(fp) != EG_STATUS_OK)
		goto error;

	for state != nil {
		if ((type = storage_read_type(fp)) == -1)
			goto error;

		switch (type)		{
			case EG_STORAGE_TYPE_USER:
				if (storage_load_user(fp) != EG_STATUS_OK) goto error;
				break;

			case EG_STORAGE_TYPE_QUEUE:
				if (storage_load_queue(fp) != EG_STATUS_OK) goto error;
				break;

			case EG_STORAGE_TYPE_ROUTE:
				if (storage_load_route(fp) != EG_STATUS_OK) goto error;
				break;

			case EG_STORAGE_TYPE_CHANNEL:
				if (storage_load_channel(fp) != EG_STATUS_OK) goto error;
				break;

			case EG_STORAGE_EOF:
				state = 0;
				break;

			default:
				goto error;
		}
	}

	fclose(fp);

	return EG_STATUS_OK;

error:
	fclose(fp);

	fatal("Error read storage %s", filename);

	return EG_STATUS_ERR;
}

func storage_save(filename string) int {
	FILE *fp;
	char tmpfile[32];

	snprintf(tmpfile, sizeof(tmpfile), "eaglemq-%d.dat", (int)getpid());

	fp = fopen(tmpfile, "w");
	if (!fp) {
		warning("Failed opening storage for saving: %s",  strerror(errno));
		return EG_STATUS_ERR;
	}

	if (storage_write_magic(fp) != EG_STATUS_OK)
		goto error;

	if (storage_save_users(fp) != EG_STATUS_OK)
		goto error;

	if (storage_save_queues(fp) != EG_STATUS_OK)
		goto error;

	if (storage_save_routes(fp) != EG_STATUS_OK)
		goto error;

	if (storage_save_channels(fp) != EG_STATUS_OK)
		goto error;

	if (storage_write_type(fp, EG_STORAGE_EOF) == -1)
		goto error;

	fflush(fp);
	fsync(fileno(fp));
	fclose(fp);

	if (rename(tmpfile, filename) == -1)
	{
		warning("Error rename temp storage file %s to %s: %s", tmpfile, filename, strerror(errno));
		unlink(tmpfile);
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;

error:
	warning("Error write data to the storage");

	fclose(fp);
	unlink(tmpfile);

	return EG_STATUS_ERR;
}

func storage_save_background(filename string) int {
	go storage_save(filename)
	return EG_STATUS_OK;
}

func remove_temp_file(pid_t pid) {
	char tmpfile[32];

	snprintf(tmpfile, sizeof(tmpfile), "eaglemq-%d.dat", (int)pid);
	os.Unlink(tmpfile);
}

// *private* functions only beyond this point

static int storage_write(FILE *fp, void *buffer, size_t length)
{
	if (fwrite(buffer, length, 1, fp) == 0)
	return -1;

	return length;
}

static int storage_read(FILE *fp, void *buffer, size_t length)
{
	if (fread(buffer, length, 1, fp) == 0)
	return -1;

	return length;
}

static int storage_write_type(FILE *fp, unsigned char type)
{
	return storage_write(fp, &type, 1);
}

static int storage_read_type(FILE *fp)
{
	unsigned char type;

	if (storage_read(fp, &type, 1) == -1)
	return -1;

	return type;
}

static int storage_write_lzf_data(FILE *fp, void *data, uint32_t length)
{
	uint32_t comprlen, outlen;
	void *out;

	if (length <= 4)
	return 0;

	outlen = length - 4;
	out = xmalloc(outlen + 1);

	comprlen = lzf_compress(data, length, out, outlen);
	if (comprlen == 0) {
		xfree(out);
		return 0;
	}

	if (storage_write(fp, &length, sizeof(length)) == -1)
	goto error;

	if (storage_write(fp, &comprlen, sizeof(comprlen)) == -1)
	goto error;

	if (storage_write(fp, out, comprlen) == -1)
	goto error;

	xfree(out);

	return 1;

error:
	xfree(out);
	return -1;
}

static int storage_write_data(FILE *fp, void *data, uint32_t length)
{
	int err;
	uint32_t comprlen = 0;

	err = storage_write_lzf_data(fp, data, length);

	if (err == -1)
	return EG_STATUS_ERR;

	if (err)
	return EG_STATUS_OK;

	if (storage_write(fp, &comprlen, sizeof(comprlen)) == -1)
	return EG_STATUS_ERR;

	if (storage_write(fp, &length, sizeof(length)) == -1)
	return EG_STATUS_ERR;

	if (storage_write(fp, data, length) == -1)
	return EG_STATUS_ERR;

	return EG_STATUS_OK;
}

static int storage_read_data(FILE *fp, void *data, uint32_t maxlen)
{
	uint32_t comprlen;
	uint32_t length;
	void *compressed;

	if (storage_read(fp, &comprlen, sizeof(comprlen)) == -1)
	return EG_STATUS_ERR;

	if (storage_read(fp, &length, sizeof(length)) == -1)
	return EG_STATUS_ERR;

	if ((length > maxlen && !comprlen) || comprlen > maxlen)
	return EG_STATUS_ERR;

	if (comprlen)
	{
		compressed = xmalloc(length);

		if (storage_read(fp, compressed, length) == -1) {
			xfree(compressed);
			return EG_STATUS_ERR;
		}

		if (lzf_decompress(compressed, length, data, comprlen) == 0) {
			xfree(compressed);
			return EG_STATUS_ERR;
		}

		xfree(compressed);
	}
	else
	{
		if (storage_read(fp, data, length) == -1)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static Object *storage_read_data_object(FILE *fp)
{
	uint32_t comprlen;
	uint32_t length;
	void *compressed;
	void *data;

	if (storage_read(fp, &comprlen, sizeof(comprlen)) == -1)
	return NULL;

	if (storage_read(fp, &length, sizeof(length)) == -1)
	return NULL;

	if (comprlen)
	{
		compressed = xmalloc(length);

		if (storage_read(fp, compressed, length) == -1) {
			xfree(compressed);
			return NULL;
		}

		data = xmalloc(comprlen);

		if (lzf_decompress(compressed, length, data, comprlen) == 0) {
			xfree(compressed);
			xfree(data);
			return NULL;
		}

		xfree(compressed);

		return create_object(data, comprlen);
	}
	else
	{
		data = xmalloc(length);

		if (storage_read(fp, data, length) == -1) {
			xfree(data);
			return NULL;
		}

		return create_object(data, length);
	}

	return NULL;
}

static int storage_write_magic(FILE *fp)
{
	char magic[16];

	snprintf(magic, sizeof(magic), "EagleMQ%04d", EG_STORAGE_VERSION);
	if (storage_write(fp, magic, 11) == -1)
	return EG_STATUS_ERR;

	return EG_STATUS_OK;
}

static int storage_read_magic(FILE *fp)
{
	char magic[16];
	int version;

	if (storage_read(fp, magic, 11) == -1)
	return EG_STATUS_ERR;

	magic[11] = '\0';

	if (memcmp(magic, "EagleMQ", 7))
	{
		warning("Bad storage signature");
		return EG_STATUS_ERR;
	}

	version = atoi(magic + 7);
	if (version < 1 || version > EG_STORAGE_VERSION) {
		warning("Bad storage version");
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_user(FILE *fp, EagleUser *user)
{
	if (storage_write_type(fp, EG_STORAGE_TYPE_USER) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, user->name, strlenz(user->name)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, user->password, strlenz(user->password)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &user->perm, sizeof(user->perm)) == -1)
	return EG_STATUS_ERR;

	return EG_STATUS_OK;
}

static int storage_save_users(FILE *fp)
{
	ListIterator iterator;
	ListNode *node;
	EagleUser *user;

	list_rewind(server->users, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		user = EG_LIST_NODE_VALUE(node);

		if (BIT_CHECK(user->perm, EG_USER_NOT_CHANGE_PERM))
		continue;

		if (storage_save_user(fp, user) != EG_STATUS_OK)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_queue_message(FILE *fp, Message *msg)
{
	if (storage_write_type(fp, EG_STORAGE_TYPE_MESSAGE) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &msg->expiration, sizeof(msg->expiration)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, EG_MESSAGE_VALUE(msg), EG_MESSAGE_SIZE(msg)) == -1)
	return EG_STATUS_ERR;

	return EG_STATUS_OK;
}

static int storage_save_queue_messages(FILE *fp, Queue_t *queue_t)
{
	QueueIterator *iterator;
	QueueNode *node;
	Message *msg;

	iterator = queue_get_iterator(queue_t->queue, EG_START_TAIL);

	while ((node = queue_next_node(iterator)) != NULL)
	{
		msg = EG_QUEUE_NODE_VALUE(node);

		if (storage_save_queue_message(fp, msg) != EG_STATUS_OK) {
			queue_release_iterator(iterator);
			return EG_STATUS_ERR;
		}
	}

	queue_release_iterator(iterator);

	return EG_STATUS_OK;
}

static int storage_save_queue(FILE *fp, Queue_t *queue_t)
{
	uint32_t queue_size = get_size_queue_t(queue_t);

	if (storage_write_type(fp, EG_STORAGE_TYPE_QUEUE) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, queue_t->name, strlenz(queue_t->name)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &queue_t->max_msg, sizeof(queue_t->max_msg)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &queue_t->max_msg_size, sizeof(queue_t->max_msg_size)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &queue_t->flags, sizeof(queue_t->flags)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &queue_size, sizeof(queue_size)) == -1)
	return EG_STATUS_ERR;

	return storage_save_queue_messages(fp, queue_t);
}

static int storage_save_queues(FILE *fp)
{
	ListIterator iterator;
	ListNode *node;
	Queue_t *queue_t;

	list_rewind(server->queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		queue_t = EG_LIST_NODE_VALUE(node);

		if (!BIT_CHECK(queue_t->flags, EG_QUEUE_DURABLE_FLAG))
		continue;

		if (storage_save_queue(fp, queue_t) != EG_STATUS_OK)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_route_key_queues(FILE *fp, List *queues)
{
	ListIterator iterator;
	ListNode *node;
	Queue_t *queue_t;

	list_rewind(queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		queue_t = EG_LIST_NODE_VALUE(node);

		if (storage_write_data(fp, queue_t->name, strlenz(queue_t->name)) == -1)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_route_key(FILE *fp, const char *key, List *queues)
{
	uint32_t queue_size = EG_LIST_LENGTH(queues);

	if (storage_write_type(fp, EG_STORAGE_TYPE_ROUTE_KEY) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, (void*)key, strlenz(key)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &queue_size, sizeof(queue_size)) == -1)
	return EG_STATUS_ERR;

	return storage_save_route_key_queues(fp, queues);
}

static int storage_save_route_keys(FILE *fp, Route_t *route)
{
	KeylistIterator iterator;
	KeylistNode *node;

	keylist_rewind(route->keys, &iterator);
	while ((node = keylist_next_node(&iterator)) != NULL)
	{
		if (storage_save_route_key(fp, EG_KEYLIST_NODE_KEY(node),
			EG_KEYLIST_NODE_VALUE(node)) != EG_STATUS_OK)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_route(FILE *fp, Route_t *route)
{
	uint32_t keys = EG_KEYLIST_LENGTH(route->keys);

	if (storage_write_type(fp, EG_STORAGE_TYPE_ROUTE) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, route->name, strlenz(route->name)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &route->flags, sizeof(route->flags)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &keys, sizeof(keys)) == -1)
	return EG_STATUS_ERR;

	return storage_save_route_keys(fp, route);
}

static int storage_save_routes(FILE *fp)
{
	ListIterator iterator;
	ListNode *node;
	Route_t *route;

	list_rewind(server->routes, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		route = EG_LIST_NODE_VALUE(node);

		if (!BIT_CHECK(route->flags, EG_ROUTE_DURABLE_FLAG))
		continue;

		if (storage_save_route(fp, route) != EG_STATUS_OK)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_save_channel(FILE *fp, Channel_t *channel)
{
	if (storage_write_type(fp, EG_STORAGE_TYPE_CHANNEL) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, channel->name, strlenz(channel->name)) == -1)
	return EG_STATUS_ERR;

	if (storage_write_data(fp, &channel->flags, sizeof(channel->flags)) == -1)
	return EG_STATUS_ERR;

	return EG_STATUS_OK;
}

static int storage_save_channels(FILE *fp)
{
	ListIterator iterator;
	ListNode *node;
	Channel_t *channel;

	list_rewind(server->channels, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		channel = EG_LIST_NODE_VALUE(node);

		if (!BIT_CHECK(channel->flags, EG_CHANNEL_DURABLE_FLAG))
		continue;

		if (storage_save_channel(fp, channel) != EG_STATUS_OK)
		return EG_STATUS_ERR;
	}

	return EG_STATUS_OK;
}

static int storage_load_user(FILE *fp)
{
	EagleUser user;

	if (storage_read_data(fp, user.name, sizeof(user.name)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, user.password, sizeof(user.password)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &user.perm, sizeof(user.perm)) == -1)
	return EG_STATUS_ERR;

	list_add_value_tail(server->users, create_user(user.name, user.password, user.perm));

	return EG_STATUS_OK;
}

static int storage_load_queue_message(FILE *fp, Queue_t *queue_t)
{
	Object *data;
	uint32_t expiration;

	if (storage_read_type(fp) != EG_STORAGE_TYPE_MESSAGE)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &expiration, sizeof(expiration)) == -1)
	return EG_STATUS_ERR;

	if ((data = storage_read_data_object(fp)) == NULL)
	return EG_STATUS_ERR;

	push_message_queue_t(queue_t, data, expiration);

	return EG_STATUS_OK;
}

static int storage_load_queue(FILE *fp)
{
	Queue_t *queue_t;
	Queue_t data;
	uint32_t queue_size;
	int i;

	if (storage_read_data(fp, &data.name, sizeof(data.name)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &data.max_msg, sizeof(data.max_msg)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &data.max_msg_size, sizeof(data.max_msg_size)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &data.flags, sizeof(data.flags)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &queue_size, sizeof(queue_size)) == -1)
	return EG_STATUS_ERR;

	queue_t = create_queue_t(data.name, data.max_msg, data.max_msg_size, data.flags);

	for (i = 0; i < queue_size; i++)
	{
		if (storage_load_queue_message(fp, queue_t) != EG_STATUS_OK) {
			delete_queue_t(queue_t);
			return EG_STATUS_ERR;
		}
	}

	list_add_value_tail(server->queues, queue_t);

	return EG_STATUS_OK;
}

static int storage_load_route_key_queues(FILE *fp, Route_t *route, const char *key)
{
	Queue_t *queue_t;
	char name[64];

	if (storage_read_data(fp, &name, sizeof(name)) == -1)
	return EG_STATUS_ERR;

	queue_t = find_queue_t(server->queues, name);
	if (!queue_t)
	return EG_STATUS_OK;

	bind_route_t(route, queue_t, key);

	return EG_STATUS_OK;
}

static int storage_load_route_key(FILE *fp, Route_t *route)
{
	char key[32];
	uint32_t queue_size;
	int i;

	if (storage_read_type(fp) != EG_STORAGE_TYPE_ROUTE_KEY)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &key, sizeof(key)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &queue_size, sizeof(queue_size)) == -1)
	return EG_STATUS_ERR;

	for (i = 0; i < queue_size; i++)
	{
		if (storage_load_route_key_queues(fp, route, key) != EG_STATUS_OK) {
			return EG_STATUS_ERR;
		}
	}

	return EG_STATUS_OK;
}

static int storage_load_route(FILE *fp)
{
	Route_t *route;
	Route_t data;
	uint32_t keys;
	int i;

	if (storage_read_data(fp, &data.name, sizeof(data.name)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &data.flags, sizeof(data.flags)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &keys, sizeof(keys)) == -1)
	return EG_STATUS_ERR;

	route = create_route_t(data.name, data.flags);

	for (i = 0; i < keys; i++)
	{
		if (storage_load_route_key(fp, route) != EG_STATUS_OK) {
			delete_route_t(route);
			return EG_STATUS_ERR;
		}
	}

	list_add_value_tail(server->routes, route);

	return EG_STATUS_OK;
}

static int storage_load_channel(FILE *fp)
{
	Channel_t *channel;
	Channel_t data;

	if (storage_read_data(fp, &data.name, sizeof(data.name)) == -1)
	return EG_STATUS_ERR;

	if (storage_read_data(fp, &data.flags, sizeof(data.flags)) == -1)
	return EG_STATUS_ERR;

	channel = create_channel_t(data.name, data.flags);

	list_add_value_tail(server->channels, channel);

	return EG_STATUS_OK;
}
