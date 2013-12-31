package eaglemq

type EagleClient struct {
	fd int
	char uint64;
	reqeust *perm
	length size_t
	pos size_t
	noack int
	offset int
	buffer []byte
	bodylen size_t
	nread size_t
	responses list.List
	declared_queues list.List
	subscribed_queues list.List
	subscribed_topics *Keylist
	subscribed_patterns *Keylist
	sentlen size_t
	last_action time.Time
}

type commandHandler func(client *EagleClient)
var commands [256]commandHandler


// Initialize the command handlers
func inits() {
	commands[EG_PROTOCOL_CMD_AUTH] = auth_command_handler;
	commands[EG_PROTOCOL_CMD_PING] = ping_command_handler;
	commands[EG_PROTOCOL_CMD_STAT] = stat_command_handler;
	commands[EG_PROTOCOL_CMD_SAVE] = save_command_handler;
	commands[EG_PROTOCOL_CMD_FLUSH] = flush_command_handler;
	commands[EG_PROTOCOL_CMD_DISCONNECT] = disconnect_command_handler;

	commands[EG_PROTOCOL_CMD_USER_CREATE] = user_create_command_handler;
	commands[EG_PROTOCOL_CMD_USER_LIST] = user_list_command_handler;
	commands[EG_PROTOCOL_CMD_USER_RENAME] = user_rename_command_handler;
	commands[EG_PROTOCOL_CMD_USER_SET_PERM] = user_set_perm_command_handler;
	commands[EG_PROTOCOL_CMD_USER_DELETE] = user_delete_command_handler;

	commands[EG_PROTOCOL_CMD_QUEUE_CREATE] = queue_create_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_DECLARE] = queue_declare_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_EXIST] = queue_exist_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_LIST] = queue_list_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_RENAME] = queue_rename_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_SIZE] = queue_size_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_PUSH] = queue_push_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_GET] = queue_get_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_POP] = queue_pop_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_CONFIRM] = queue_confirm_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_SUBSCRIBE] = queue_subscribe_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_UNSUBSCRIBE] = queue_unsubscribe_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_PURGE] = queue_purge_command_handler;
	commands[EG_PROTOCOL_CMD_QUEUE_DELETE] = queue_delete_command_handler;

	commands[EG_PROTOCOL_CMD_ROUTE_CREATE] = route_create_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_EXIST] = route_exist_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_LIST] = route_list_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_KEYS] = route_keys_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_RENAME] = route_rename_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_BIND] = route_bind_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_UNBIND] = route_unbind_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_PUSH] = route_push_command_handler;
	commands[EG_PROTOCOL_CMD_ROUTE_DELETE] = route_delete_command_handler;

	commands[EG_PROTOCOL_CMD_CHANNEL_CREATE] = channel_create_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_EXIST] = channel_exist_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_LIST] = channel_list_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_RENAME] = channel_rename_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_PUBLISH] = channel_publish_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_SUBSCRIBE] = channel_subscribe_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_PSUBSCRIBE] = channel_psubscribe_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_UNSUBSCRIBE] = channel_unsubscribe_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_PUNSUBSCRIBE] = channel_punsubscribe_command_handler;
	commands[EG_PROTOCOL_CMD_CHANNEL_DELETE] = channel_delete_command_handler;
}


func auth_command_handler(client *EagleClient) {
	ProtocolRequestAuth *req = (ProtocolRequestAuth*)client->request;
	EagleUser *user;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!check_input_buffer2(req->body.name, 32) || !check_input_buffer1(req->body.password, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	user = find_user(server->users, req->body.name, req->body.password);
	if (!user) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	client->perm = user->perm;

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func ping_command_handler(client *EagleClient) {
	ProtocolRequestPing *req = (ProtocolRequestPing*)client->request;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func stat_command_handler(client *EagleClient) {
	ProtocolRequestStat *req = (ProtocolRequestStat*)client->request;
	ProtocolResponseStat *stat;
	struct rusage self_ru, c_ru;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	getrusage(RUSAGE_SELF, &self_ru);
	getrusage(RUSAGE_CHILDREN, &c_ru);

	stat = (ProtocolResponseStat*)xcalloc(sizeof(*stat));

	set_response_header(&stat->header, req->cmd, EG_PROTOCOL_STATUS_SUCCESS, sizeof(stat->body));

	stat->body.version.major = EAGLE_VERSION_MAJOR;
	stat->body.version.minor = EAGLE_VERSION_MINOR;
	stat->body.version.patch = EAGLE_VERSION_PATCH;

	stat->body.uptime = time(NULL) - server->start_time;
	stat->body.used_cpu_sys = (float)self_ru.ru_stime.tv_sec + (float)self_ru.ru_stime.tv_usec/1000000;
	stat->body.used_cpu_user = (float)self_ru.ru_utime.tv_sec + (float)self_ru.ru_utime.tv_usec/1000000;
	stat->body.used_memory = xmalloc_used_memory();
	stat->body.used_memory_rss = xmalloc_memory_rss();
	stat->body.fragmentation_ratio = xmalloc_fragmentation_ratio();
	stat->body.clients = EG_LIST_LENGTH(server->clients);
	stat->body.users = EG_LIST_LENGTH(server->users);
	stat->body.queues = EG_LIST_LENGTH(server->queues);
	stat->body.routes = EG_LIST_LENGTH(server->routes);
	stat->body.channels = EG_LIST_LENGTH(server->channels);
	stat->body.resv3 = 0;
	stat->body.resv4 = 0;

	add_response(client, stat, sizeof(*stat));
}

func save_command_handler(client *EagleClient){
	ProtocolRequestSave *req = (ProtocolRequestSave*)client->request;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (req->body.async)
	{
		if (storage_save_background(server->storage) != EG_STATUS_OK) {
			add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		}
	}
	else
	{
		if (storage_save(server->storage) != EG_STATUS_OK) {
			add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		}
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func flush_command_handler(client *EagleClient) {
	ProtocolRequestFlush *req = (ProtocolRequestFlush*)client->request;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (BIT_CHECK(req->body.flags, EG_FLUSH_USER_FLAG)) {
		delete_users();
	}

	if (BIT_CHECK(req->body.flags, EG_FLUSH_QUEUE_FLAG)) {
		delete_queues();
	}

	if (BIT_CHECK(req->body.flags, EG_FLUSH_ROUTE_FLAG)) {
		delete_routes();
	}

	if (BIT_CHECK(req->body.flags, EG_FLUSH_CHANNEL_FLAG)) {
		delete_channels();
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func disconnect_command_handler(client *EagleClient) {
	ProtocolRequestDisconnect *req = (ProtocolRequestDisconnect*)client->request;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	free_client(client);
}

func user_create_command_handler(client *EagleClient) {
	ProtocolRequestUserCreate *req = (ProtocolRequestUserCreate*)client->request;
	EagleUser *user;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 32) || !check_input_buffer1(req->body.password, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (find_user(server->users, req->body.name, NULL)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	user = create_user(req->body.name, req->body.password, req->body.perm);
	list_add_value_tail(server->users, user);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func user_list_command_handler(client *EagleClient){
	ProtocolRequestUserList *req = (ProtocolRequestUserList*)client->request;
	ProtocolResponseHeader res;
	EagleUser *user;
	ListIterator iterator;
	ListNode *node;
	char *list;
	int i;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	set_response_header(&res, req->cmd, EG_PROTOCOL_STATUS_SUCCESS,
		EG_LIST_LENGTH(server->users) * (64 + sizeof(uint64_t)));

	list = (char*)xcalloc(sizeof(res) + res.bodylen);

	memcpy(list, &res, sizeof(res));

	i = sizeof(res);
	list_rewind(server->users, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		user = EG_LIST_NODE_VALUE(node);
		memcpy(list + i, user->name, strlenz(user->name));
		memcpy(list + i + 32, user->password, strlenz(user->password));
		memcpy(list + i + 64, &user->perm, sizeof(uint64_t));
		i += 64 + sizeof(uint64_t);
	}

	add_response(client, list, sizeof(res) + res.bodylen);
}

func user_rename_command_handler(client *EagleClient){
	ProtocolRequestUserRename *req = (ProtocolRequestUserRename*)client->request;
	EagleUser *user;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.from, 32) || !check_input_buffer2(req->body.to, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	user = find_user(server->users, req->body.from, NULL);
	if (!user || find_user(server->users, req->body.to, NULL)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (BIT_CHECK(user->perm, EG_USER_NOT_CHANGE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	rename_user(user, req->body.to);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

func user_set_perm_command_handler(client *EagleClient){
	ProtocolRequestUserSetPerm *req = (ProtocolRequestUserSetPerm*)client->request;
	EagleUser *user;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	user = find_user(server->users, req->body.name, NULL);
	if (!user) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (BIT_CHECK(user->perm, EG_USER_NOT_CHANGE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	set_user_perm(user, req->body.perm);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void user_delete_command_handler(client *EagleClient)
{
	ProtocolRequestUserDelete *req = (ProtocolRequestUserDelete*)client->request;
	EagleUser *user;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	user = find_user(server->users, req->body.name, NULL);
	if (!user) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (BIT_CHECK(user->perm, EG_USER_NOT_CHANGE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	if (list_delete_value(server->users, user) == EG_STATUS_ERR) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_create_command_handler(client *EagleClient)
{
	ProtocolRequestQueueCreate *req = (ProtocolRequestQueueCreate*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_CREATE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (find_queue_t(server->queues, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	if (!req->body.max_msg || req->body.max_msg_size > EG_MAX_MSG_SIZE) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = create_queue_t(req->body.name, req->body.max_msg,
		((req->body.max_msg_size == 0) ? EG_MAX_MSG_SIZE : req->body.max_msg_size), req->body.flags);

	list_add_value_tail(server->queues, queue_t);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_declare_command_handler(client *EagleClient)
{
	ProtocolRequestQueueDeclare *req = (ProtocolRequestQueueDeclare*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_DECLARE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (find_queue_t(client->declared_queues, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
		return;
	}

	declare_client_queue_t(queue_t, client);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_exist_command_handler(client *EagleClient)
{
	ProtocolRequestQueueExist *req = (ProtocolRequestQueueExist*)client->request;
	ProtocolResponseQueueExist *res;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_EXIST_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	res = (ProtocolResponseQueueExist*)xcalloc(sizeof(*res));

	set_response_header(&res->header, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, sizeof(res->body));

	if (find_queue_t(server->queues, req->body.name)) {
		res->body.status = 1;
	} else {
		res->body.status = 0;
	}

	add_response(client, res, sizeof(*res));
}

void queue_list_command_handler(client *EagleClient)
{
	ProtocolRequestQueueList *req = (ProtocolRequestQueueList*)client->request;
	ProtocolResponseHeader res;
	ListIterator iterator;
	ListNode *node;
	Queue_t *queue_t;
	uint32_t queue_size, declared_clients, subscribed_clients;
	char *list;
	int i;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_LIST_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	set_response_header(&res, req->cmd, EG_PROTOCOL_STATUS_SUCCESS,
		EG_LIST_LENGTH(server->queues) * (64 + (sizeof(uint32_t) * 6)));

	list = (char*)xcalloc(sizeof(res) + res.bodylen);

	memcpy(list, &res, sizeof(res));

	i = sizeof(res);
	list_rewind(server->queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		queue_t = EG_LIST_NODE_VALUE(node);

		queue_size = get_size_queue_t(queue_t);
		declared_clients = get_declared_clients_queue_t(queue_t);
		subscribed_clients = get_subscribed_clients_queue_t(queue_t);

		memcpy(list + i, queue_t->name, strlenz(queue_t->name));
		i += 64;
		memcpy(list + i, &queue_t->max_msg, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &queue_t->max_msg_size, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &queue_t->flags, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &queue_size, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &declared_clients, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &subscribed_clients, sizeof(uint32_t));
		i += sizeof(uint32_t);
	}

	add_response(client, list, sizeof(res) + res.bodylen);
}

void queue_rename_command_handler(client *EagleClient)
{
	ProtocolRequestQueueRename *req = (ProtocolRequestQueueRename*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_RENAME_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.from, 64) || !check_input_buffer2(req->body.to, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.from);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (find_queue_t(server->queues, req->body.to)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	rename_queue_t(queue_t, req->body.to);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_size_command_handler(client *EagleClient)
{
	ProtocolRequestQueueSize *req = (ProtocolRequestQueueSize*)client->request;
	ProtocolResponseQueueSize *res;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_SIZE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	res = (ProtocolResponseQueueSize*)xmalloc(sizeof(*res));

	set_response_header(&res->header, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, sizeof(res->body));

	res->body.size = get_size_queue_t(queue_t);

	add_response(client, res, sizeof(*res));
}

void queue_push_command_handler(client *EagleClient)
{
	ProtocolRequestHeader *req = (ProtocolRequestHeader*)client->request;
	Queue_t *queue_t;
	Object *msg;
	char *queue_name, *msg_data;
	uint32_t expire;
	size_t msg_size;

	if (client->pos < (sizeof(*req) + 69)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_PUSH_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	queue_name = client->request + sizeof(*req);

	if (!check_input_buffer2(queue_name, 64)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (server->nomemory) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_MEMORY);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, queue_name);
	if (!queue_t) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	msg_data = client->request + sizeof(*req) + 64 + sizeof(uint32_t);
	msg_size = client->pos - (sizeof(*req) + 64 + sizeof(uint32_t));
	expire = *((uint32_t*)(client->request + sizeof(*req) + 64));

	if (expire) {
		expire += server->now_timems;
	}

	msg = create_dup_object(msg_data, msg_size);

	if (push_message_queue_t(queue_t, msg, expire) == EG_STATUS_ERR) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_get_command_handler(client *EagleClient)
{
	ProtocolRequestQueueGet *req = (ProtocolRequestQueueGet*)client->request;
	ProtocolResponseHeader res;
	Queue_t *queue_t;
	Message *msg;
	char *buffer;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_GET_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	msg = get_message_queue_t(queue_t);
	if (!msg) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NO_DATA);
		return;
	}

	set_response_header(&res, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, EG_MESSAGE_SIZE(msg) + sizeof(uint64_t));

	buffer = (char*)xmalloc(sizeof(res) + sizeof(uint64_t));

	memcpy(buffer, &res, sizeof(res));
	memcpy(buffer + sizeof(res), &msg->tag, sizeof(uint64_t));

	add_response(client, buffer, sizeof(res) + sizeof(uint64_t));
	add_object_response(client, EG_MESSAGE_OBJECT(msg));
}

void queue_pop_command_handler(client *EagleClient)
{
	ProtocolRequestQueuePop *req = (ProtocolRequestQueuePop*)client->request;
	ProtocolResponseHeader res;
	Queue_t *queue_t;
	Message *msg;
	char *buffer;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_POP_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	msg = get_message_queue_t(queue_t);
	if (!msg) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NO_DATA);
		return;
	}

	set_response_header(&res, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, EG_MESSAGE_SIZE(msg) + sizeof(uint64_t));

	buffer = (char*)xmalloc(sizeof(res) + sizeof(uint64_t));

	memcpy(buffer, &res, sizeof(res));
	memcpy(buffer + sizeof(res), &msg->tag, sizeof(uint64_t));

	add_response(client, buffer, sizeof(res) + sizeof(uint64_t));
	add_object_response(client, EG_MESSAGE_OBJECT(msg));

	pop_message_queue_t(queue_t, req->body.timeout);
}

void queue_confirm_command_handler(client *EagleClient)
{
	ProtocolRequestQueueConfirm *req = (ProtocolRequestQueueConfirm*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_CONFIRM_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	if (confirm_message_queue_t(queue_t, req->body.tag) != EG_STATUS_OK) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NO_DATA);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_subscribe_command_handler(client *EagleClient)
{
	ProtocolRequestQueueSubscribe *req = (ProtocolRequestQueueSubscribe*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_SUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	if (find_queue_t(client->subscribed_queues, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	subscribe_client_queue_t(queue_t, client, req->body.flags);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_unsubscribe_command_handler(client *EagleClient)
{
	ProtocolRequestQueueUnsubscribe *req = (ProtocolRequestQueueUnsubscribe*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_UNSUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	if (!find_queue_t(client->subscribed_queues, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	unsubscribe_client_queue_t(queue_t, client);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_purge_command_handler(client *EagleClient)
{
	ProtocolRequestQueuePurge *req = (ProtocolRequestQueuePurge*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_PURGE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(client->declared_queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_DECLARED);
		return;
	}

	purge_queue_t(queue_t);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_delete_command_handler(client *EagleClient)
{
	ProtocolRequestQueueDelete *req = (ProtocolRequestQueueDelete*)client->request;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_QUEUE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_QUEUE_DELETE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.name);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (list_delete_value(server->queues, queue_t) == EG_STATUS_ERR) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_create_command_handler(client *EagleClient)
{
	ProtocolRequestRouteCreate *req = (ProtocolRequestRouteCreate*)client->request;
	Route_t *route;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_CREATE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (find_route_t(server->routes, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	route = create_route_t(req->body.name, req->body.flags);

	list_add_value_tail(server->routes, route);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_exist_command_handler(client *EagleClient)
{
	ProtocolRequestRouteExist *req = (ProtocolRequestRouteExist*)client->request;
	ProtocolResponseRouteExist *res;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_EXIST_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	res = (ProtocolResponseRouteExist*)xcalloc(sizeof(*res));

	set_response_header(&res->header, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, sizeof(res->body));

	if (find_route_t(server->routes, req->body.name)) {
		res->body.status = 1;
	} else {
		res->body.status = 0;
	}

	add_response(client, res, sizeof(*res));
}

void route_list_command_handler(client *EagleClient)
{
	ProtocolRequestRouteList *req = (ProtocolRequestRouteList*)client->request;
	ProtocolResponseHeader res;
	ListIterator iterator;
	ListNode *node;
	Route_t *route;
	char *list;
	uint32_t keys;
	int i;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_LIST_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	set_response_header(&res, req->cmd, EG_PROTOCOL_STATUS_SUCCESS,
		EG_LIST_LENGTH(server->routes) * (64 + (sizeof(uint32_t) * 2)));

	list = (char*)xcalloc(sizeof(res) + res.bodylen);

	memcpy(list, &res, sizeof(res));

	i = sizeof(res);
	list_rewind(server->routes, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		route = EG_LIST_NODE_VALUE(node);

		keys = EG_KEYLIST_LENGTH(route->keys);

		memcpy(list + i, route->name, strlenz(route->name));
		i += 64;
		memcpy(list + i, &route->flags, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &keys, sizeof(uint32_t));
		i += sizeof(uint32_t);
	}

	add_response(client, list, sizeof(res) + res.bodylen);
}

void route_keys_command_handler(client *EagleClient)
{
	ProtocolRequestRouteKeys *req = (ProtocolRequestRouteKeys*)client->request;
	ProtocolResponseHeader res;
	KeylistIterator keylist_iterator;
	KeylistNode *keylist_node;
	ListIterator list_iterator;
	ListNode *list_node;
	Route_t *route;
	Queue_t *queue_t;
	char *key;
	List *queues;
	char *list;
	int i;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_KEYS_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	route = find_route_t(server->routes, req->body.name);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	set_response_header(&res, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS,
		get_queue_number_route_t(route) * (32 + 64));

	list = (char*)xcalloc(sizeof(res) + res.bodylen);

	memcpy(list, &res, sizeof(res));

	i = sizeof(res);
	keylist_rewind(route->keys, &keylist_iterator);
	while ((keylist_node = keylist_next_node(&keylist_iterator)) != NULL)
	{
		key = EG_KEYLIST_NODE_KEY(keylist_node);
		queues = EG_KEYLIST_NODE_VALUE(keylist_node);

		list_rewind(queues, &list_iterator);
		while ((list_node = list_next_node(&list_iterator)) != NULL)
		{
			queue_t = EG_LIST_NODE_VALUE(list_node);

			memcpy(list + i, key, strlenz(key));
			i += 32;
			memcpy(list + i, queue_t->name, strlenz(queue_t->name));
			i += 64;
		}
	}

	add_response(client, list, sizeof(res) + res.bodylen);
}

void route_rename_command_handler(client *EagleClient)
{
	ProtocolRequestRouteRename *req = (ProtocolRequestRouteRename*)client->request;
	Route_t *route;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_RENAME_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.from, 64) || !check_input_buffer2(req->body.to, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	route = find_route_t(server->routes, req->body.from);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (find_route_t(server->routes, req->body.to)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	rename_route_t(route, req->body.to);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_bind_command_handler(client *EagleClient)
{
	ProtocolRequestRouteBind *req = (ProtocolRequestRouteBind*)client->request;
	Route_t *route;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_BIND_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer2(req->body.queue, 64)
		|| !check_input_buffer1(req->body.key, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	route = find_route_t(server->routes, req->body.name);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.queue);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	bind_route_t(route, queue_t, req->body.key);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_unbind_command_handler(client *EagleClient)
{
	ProtocolRequestRouteUnbind *req = (ProtocolRequestRouteUnbind*)client->request;
	Route_t *route;
	Queue_t *queue_t;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_UNBIND_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer2(req->body.queue, 64)
		|| !check_input_buffer1(req->body.key, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	route = find_route_t(server->routes, req->body.name);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	queue_t = find_queue_t(server->queues, req->body.queue);
	if (!queue_t) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (unbind_route_t(route, queue_t, req->body.key) == EG_STATUS_ERR) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_push_command_handler(client *EagleClient)
{
	ProtocolRequestRoutePush *req = (ProtocolRequestRoutePush*)client->request;
	Route_t *route;
	Object *msg;
	char *msg_data;
	uint32_t expire;
	size_t msg_size;

	if (client->pos < (sizeof(*req) + 5)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_PUSH_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer1(req->body.key, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (server->nomemory) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_MEMORY);
		return;
	}

	route = find_route_t(server->routes, req->body.name);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	msg_data = client->request + sizeof(*req) + sizeof(uint32_t);
	msg_size = client->pos - (sizeof(*req) + sizeof(uint32_t));
	expire = *((uint32_t*)(client->request + sizeof(*req)));

	if (expire) {
		expire += server->now_timems;
	}

	msg = create_dup_object(msg_data, msg_size);
	EG_OBJECT_RESET_REFCOUNT(msg);

	if (push_message_route_t(route, req->body.key, msg, expire) != EG_STATUS_OK) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void route_delete_command_handler(client *EagleClient)
{
	ProtocolRequestRouteDelete *req = (ProtocolRequestRouteDelete*)client->request;
	Route_t *route;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_ROUTE_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_ROUTE_DELETE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	route = find_route_t(server->routes, req->body.name);
	if (!route) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (list_delete_value(server->routes, route) == EG_STATUS_ERR) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_create_command_handler(client *EagleClient)
{
	ProtocolRequestChannelCreate *req = (ProtocolRequestChannelCreate*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_CREATE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	if (find_channel_t(server->channels, req->body.name)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	channel = create_channel_t(req->body.name, req->body.flags);

	list_add_value_tail(server->channels, channel);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_exist_command_handler(client *EagleClient)
{
	ProtocolRequestChannelExist *req = (ProtocolRequestChannelExist*)client->request;
	ProtocolResponseChannelExist *res;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_EXIST_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	res = (ProtocolResponseChannelExist*)xcalloc(sizeof(*res));

	set_response_header(&res->header, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS, sizeof(res->body));

	if (find_channel_t(server->channels, req->body.name)) {
		res->body.status = 1;
	} else {
		res->body.status = 0;
	}

	add_response(client, res, sizeof(*res));
}

void channel_list_command_handler(client *EagleClient)
{
	ProtocolRequestChannelList *req = (ProtocolRequestChannelList*)client->request;
	ProtocolResponseHeader res;
	ListIterator iterator;
	ListNode *node;
	Channel_t *channel;
	char *list;
	uint32_t topics;
	uint32_t patterns;
	int i;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_LIST_PERM)) {
		add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	set_response_header(&res, req->cmd, EG_PROTOCOL_STATUS_SUCCESS,
		EG_LIST_LENGTH(server->channels) * (64 + (sizeof(uint32_t) * 3)));

	list = (char*)xcalloc(sizeof(res) + res.bodylen);

	memcpy(list, &res, sizeof(res));

	i = sizeof(res);
	list_rewind(server->channels, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		channel = EG_LIST_NODE_VALUE(node);

		topics = EG_KEYLIST_LENGTH(channel->topics);
		patterns = EG_KEYLIST_LENGTH(channel->patterns);

		memcpy(list + i, channel->name, strlenz(channel->name));
		i += 64;
		memcpy(list + i, &channel->flags, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &topics, sizeof(uint32_t));
		i += sizeof(uint32_t);
		memcpy(list + i, &patterns, sizeof(uint32_t));
		i += sizeof(uint32_t);
	}

	add_response(client, list, sizeof(res) + res.bodylen);
}

void channel_rename_command_handler(client *EagleClient)
{
	ProtocolRequestChannelRename *req = (ProtocolRequestChannelRename*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_RENAME_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.from, 64) || !check_input_buffer2(req->body.to, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.from);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (find_channel_t(server->channels, req->body.to)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	rename_channel_t(channel, req->body.to);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_publish_command_handler(client *EagleClient)
{
	ProtocolRequestChannelPublish *req = (ProtocolRequestChannelPublish*)client->request;
	Channel_t *channel;
	Object *msg;

	if (client->pos < (sizeof(*req) + 1)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_PUBLISH_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer1(req->body.topic, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	msg = create_dup_object(client->request + sizeof(*req), client->pos - sizeof(*req));

	publish_message_channel_t(channel, req->body.topic, msg);

	decrement_references_count(msg);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_subscribe_command_handler(client *EagleClient)
{
	ProtocolRequestChannelSubscribe *req = (ProtocolRequestChannelSubscribe*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_SUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer1(req->body.topic, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	subscribe_channel_t(channel, client, req->body.topic);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_psubscribe_command_handler(client *EagleClient)
{
	ProtocolRequestChannelPatternSubscribe *req = (ProtocolRequestChannelPatternSubscribe*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_PSUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer3(req->body.pattern, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	psubscribe_channel_t(channel, client, req->body.pattern);

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_unsubscribe_command_handler(client *EagleClient)
{
	ProtocolRequestChannelUnsubscribe *req = (ProtocolRequestChannelUnsubscribe*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_UNSUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer1(req->body.topic, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (unsubscribe_channel_t(channel, client, req->body.topic) != EG_STATUS_OK) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_punsubscribe_command_handler(client *EagleClient)
{
	ProtocolRequestChannelPatternUnsubscribe *req = (ProtocolRequestChannelPatternUnsubscribe*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_PUNSUBSCRIBE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64) || !check_input_buffer3(req->body.pattern, 32)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (punsubscribe_channel_t(channel, client, req->body.pattern) != EG_STATUS_OK) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void channel_delete_command_handler(client *EagleClient)
{
	ProtocolRequestChannelDelete *req = (ProtocolRequestChannelDelete*)client->request;
	Channel_t *channel;

	if (client->pos < sizeof(*req)) {
		add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
		return;
	}

	if (!BIT_CHECK(client->perm, EG_USER_ADMIN_PERM) && !BIT_CHECK(client->perm, EG_USER_CHANNEL_PERM)
		&& !BIT_CHECK(client->perm, EG_USER_CHANNEL_DELETE_PERM)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_ACCESS);
		return;
	}

	if (!check_input_buffer2(req->body.name, 64)) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_VALUE);
		return;
	}

	channel = find_channel_t(server->channels, req->body.name);
	if (!channel) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR_NOT_FOUND);
		return;
	}

	if (list_delete_value(server->channels, channel) == EG_STATUS_ERR) {
		add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_ERROR);
		return;
	}

	add_status_response(client, req->header.cmd, EG_PROTOCOL_STATUS_SUCCESS);
}

void queue_client_event_notify(EagleClient *client, Queue_t *queue_t)
{
	ProtocolEventQueueNotify *event = (ProtocolEventQueueNotify*)xcalloc(sizeof(*event));

	set_event_header(&event->header, EG_PROTOCOL_CMD_QUEUE_SUBSCRIBE, EG_PROTOCOL_EVENT_NOTIFY, sizeof(event->body));

	memcpy(event->body.name, queue_t->name, strlenz(queue_t->name));

	add_response(client, event, sizeof(*event));
}

void queue_client_event_message(EagleClient *client, Queue_t *queue_t, Message *msg)
{
	ProtocolEventHeader header;
	char *buffer;

	set_event_header(&header, EG_PROTOCOL_CMD_QUEUE_SUBSCRIBE, EG_PROTOCOL_EVENT_MESSAGE, 64 + EG_MESSAGE_SIZE(msg));

	buffer = (char*)xcalloc(sizeof(header) + 64);

	memcpy(buffer, &header, sizeof(header));
	memcpy(buffer + sizeof(header), queue_t->name, strlenz(queue_t->name));

	add_response(client, buffer, sizeof(header) + 64);
	add_object_response(client, EG_MESSAGE_OBJECT(msg));
}

void channel_client_event_message(EagleClient *client, Channel_t *channel, const char *topic, Object *msg)
{
	ProtocolEventHeader header;
	char *buffer;

	set_event_header(&header, EG_PROTOCOL_CMD_CHANNEL_SUBSCRIBE, EG_PROTOCOL_EVENT_MESSAGE, 96 + EG_OBJECT_SIZE(msg));

	buffer = (char*)xcalloc(sizeof(header) + 96);

	memcpy(buffer, &header, sizeof(header));
	memcpy(buffer + sizeof(header), channel->name, strlenz(channel->name));
	memcpy(buffer + sizeof(header) + 64, topic, strlenz(topic));

	add_response(client, buffer, sizeof(header) + 96);
	add_object_response(client, msg);
}

void channel_client_event_pattern_message(EagleClient *client, Channel_t *channel,
	const char *topic, const char *pattern, Object *msg)
{
	ProtocolEventHeader header;
	char *buffer;

	set_event_header(&header, EG_PROTOCOL_CMD_CHANNEL_PSUBSCRIBE, EG_PROTOCOL_EVENT_MESSAGE, 128 + EG_OBJECT_SIZE(msg));

	buffer = (char*)xcalloc(sizeof(header) + 128);

	memcpy(buffer, &header, sizeof(header));
	memcpy(buffer + sizeof(header), channel->name, strlenz(channel->name));
	memcpy(buffer + sizeof(header) + 64, topic, strlenz(topic));
	memcpy(buffer + sizeof(header) + 96, pattern, strlenz(pattern));

	add_response(client, buffer, sizeof(header) + 128);
	add_object_response(client, msg);
}

void process_request(client *EagleClient)
{
	ProtocolRequestHeader *req;

process:
	if (!client->bodylen)
	{
		if (client->nread < sizeof(*req)) {
			add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
			return;
		}

		req = (ProtocolRequestHeader*)(client->buffer + client->offset);

		if (req->magic != EG_PROTOCOL_REQ)
		{
			client->offset = 0;
			client->bodylen = 0;
			add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
			return;
		}

		if (req->bodylen > EG_MAX_BUF_SIZE)
		{
			client->offset = 0;
			client->bodylen = 0;
			add_status_response(client, 0, EG_PROTOCOL_STATUS_ERROR_PACKET);
			return;
		}

		if (client->length < req->bodylen) {
			client->request = xrealloc(client->request, EG_MAX_BUF_SIZE);
			client->length = EG_MAX_BUF_SIZE;
		}

		int delta = client->nread - sizeof(*req);

		if (delta == (int)req->bodylen)
		{
			memcpy(client->request, client->buffer + client->offset, client->nread);
			client->pos = client->nread;
			client->offset = 0;
			client->nread = 0;
		}
		else if (delta < (int)req->bodylen)
		{
			memcpy(client->request, client->buffer + client->offset, client->nread);
			client->bodylen = req->bodylen - delta;
			client->pos = client->nread;
			return;
		}
		else if (delta > (int)req->bodylen)
		{
			client->pos = req->bodylen + sizeof(*req);
			memcpy(client->request, client->buffer + client->offset, client->pos);
			client->offset += client->pos;
			client->nread -= client->pos;
		}
	} else {
		if (client->nread == client->bodylen)
		{
			memcpy(client->request + client->pos, client->buffer, client->nread);
			client->pos += client->nread;
			client->offset = 0;
			client->nread = 0;
			client->bodylen = 0;
		}
		else if (client->nread < client->bodylen)
		{
			memcpy(client->request + client->pos, client->buffer, client->nread);
			client->pos += client->nread;
			client->bodylen -= client->nread;
			client->offset = 0;
			return;
		}
		else if (client->nread > client->bodylen)
		{
			memcpy(client->request + client->pos, client->buffer, client->bodylen);
			client->pos += client->bodylen;
			client->nread -= client->bodylen;
			client->offset = client->bodylen;
			client->bodylen = 0;
		}
	}

	req = (ProtocolRequestHeader*)client->request;
	parse_command(client, req);

	if ((int)client->nread >= (int)sizeof(*req)) {
		goto process;
	}
}

void read_request(EventLoop *loop, int fd, void *data, int mask)
{
	EagleClient *client = (EagleClient*)data;
	int nread;

	EG_NOTUSED(loop);
	EG_NOTUSED(mask);

	nread = read(fd, client->buffer, EG_BUF_SIZE);

	if (nread == -1) {
		if (errno == EAGAIN) {
			nread = 0;
		} else {
			free_client(client);
			return;
		}
	} else if (nread == 0) {
		free_client(client);
		return;
	}

	if (nread) {
		client->nread = nread;
		client->last_action = time(NULL);
	} else {
		return;
	}

	process_request(client);
}


EagleClient *create_client(int fd)
{
	EagleClient *client = (EagleClient*)xmalloc(sizeof(*client));

	net_set_nonblock(NULL, fd);
	net_tcp_nodelay(NULL, fd);

	if (create_file_event(server->loop, fd, EG_EVENT_READABLE, read_request, client) == EG_EVENT_ERR) {
		close(fd);
		xfree(client);
		return NULL;
	}

	client->fd = fd;
	client->perm = 0;
	client->request = (char*)xmalloc(EG_BUF_SIZE);
	client->length = EG_BUF_SIZE;
	client->pos = 0;
	client->noack = 0;
	client->offset = 0;
	client->buffer = (char*)xmalloc(EG_BUF_SIZE);
	client->bodylen = 0;
	client->nread = 0;
	client->responses = list_create();
	client->declared_queues = list_create();
	client->subscribed_queues = list_create();
	client->subscribed_topics = keylist_create();
	client->subscribed_patterns = keylist_create();
	client->sentlen = 0;
	client->last_action = time(NULL);

	EG_LIST_SET_FREE_METHOD(client->responses, free_object_list_handler);

	list_add_value_tail(server->clients, client);

	return client;
}

void free_client(client *EagleClient)
{
	xfree(client->request);
	xfree(client->buffer);

	eject_queue_client(client);
	eject_channel_client(client);

	list_release(client->responses);
	list_release(client->declared_queues);
	list_release(client->subscribed_queues);
	keylist_release(client->subscribed_topics);
	keylist_release(client->subscribed_patterns);

	delete_file_event(server->loop, client->fd, EG_EVENT_READABLE);
	delete_file_event(server->loop, client->fd, EG_EVENT_WRITABLE);

	close(client->fd);

	list_delete_value(server->clients, client);

	xfree(client);
}

void accept_tcp_handler(EventLoop *loop, int fd, void *data, int mask)
{
	int port, cfd;
	char ip[128];

	EG_NOTUSED(loop);
	EG_NOTUSED(mask);
	EG_NOTUSED(data);

	cfd = net_tcp_accept(server->error, fd, ip, &port);
	if (cfd == EG_NET_ERR) {
		warning("Error accept client: %s", server->error);
		return;
	}

	accept_common_handler(cfd);
}

void accept_unix_handler(EventLoop *loop, int fd, void *data, int mask)
{
	int cfd;

	EG_NOTUSED(loop);
	EG_NOTUSED(mask);
	EG_NOTUSED(data);

	cfd = net_unix_accept(server->error, fd);
	if (cfd == EG_NET_ERR) {
		warning("Accepting client connection: %s", server->error);
		return;
	}

	accept_common_handler(cfd);
}

// *private* functions only beyond this point

static void accept_common_handler(int fd)
{
	EagleClient *client;

	if ((client = create_client(fd)) == NULL) {
		close(fd);
		return;
	}

	if (EG_LIST_LENGTH(server->clients) > server->max_clients) {
		free_client(client);
	}
}

static inline void set_response_header(ProtocolResponseHeader *header, uint8_t cmd, uint8_t status, uint32_t bodylen)
{
	header->magic = EG_PROTOCOL_RES;
	header->cmd = cmd;
	header->status = status;
	header->bodylen = bodylen;
}

static inline void set_event_header(ProtocolEventHeader *header, uint8_t cmd, uint8_t type, uint32_t bodylen)
{
	header->magic = EG_PROTOCOL_EVENT;
	header->cmd = cmd;
	header->type = type;
	header->bodylen = bodylen;
}

static void eject_queue_client(client *EagleClient)
{
	ListIterator iterator;
	ListNode *node;
	Queue_t *queue_t;

	list_rewind(client->subscribed_queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		queue_t = EG_LIST_NODE_VALUE(node);
		unsubscribe_client_queue_t(queue_t, client);
	}

	list_rewind(client->declared_queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		queue_t = EG_LIST_NODE_VALUE(node);

		undeclare_client_queue_t(queue_t, client);
		process_queue_t(queue_t);
	}
}

static void eject_channel_client(client *EagleClient)
{
	ListIterator list_iterator;
	ListNode *list_node;
	KeylistIterator keylist_iterator;
	KeylistNode *keylist_node;
	Channel_t *channel;
	List *list;

	keylist_rewind(client->subscribed_topics, &keylist_iterator);
	while ((keylist_node = keylist_next_node(&keylist_iterator)) != NULL)
	{
		channel = EG_KEYLIST_NODE_KEY(keylist_node);
		list = EG_KEYLIST_NODE_VALUE(keylist_node);

		list_rewind(list, &list_iterator);
		while ((list_node = list_next_node(&list_iterator)) != NULL)
		{
			keylist_node = EG_LIST_NODE_VALUE(list_node);
			unsubscribe_channel_t(channel, client, EG_KEYLIST_NODE_KEY(keylist_node));
		}
	}

	keylist_rewind(client->subscribed_patterns, &keylist_iterator);
	while ((keylist_node = keylist_next_node(&keylist_iterator)) != NULL)
	{
		channel = EG_KEYLIST_NODE_KEY(keylist_node);
		list = EG_KEYLIST_NODE_VALUE(keylist_node);

		list_rewind(list, &list_iterator);
		while ((list_node = list_next_node(&list_iterator)) != NULL)
		{
			keylist_node = EG_LIST_NODE_VALUE(list_node);
			punsubscribe_channel_t(channel, client, EG_KEYLIST_NODE_KEY(keylist_node));
		}
	}
}

static void delete_users(void)
{
	ListIterator iterator;
	ListNode *node;
	EagleUser *user;

	list_rewind(server->users, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		user = EG_LIST_NODE_VALUE(node);

		if (BIT_CHECK(user->perm, EG_USER_NOT_CHANGE_PERM)) {
			continue;
		}

		list_delete_node(server->users, node);
	}
}

static void delete_queues(void)
{
	ListIterator iterator;
	ListNode *node;

	list_rewind(server->queues, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		list_delete_node(server->queues, node);
	}
}

static void delete_routes(void)
{
	ListIterator iterator;
	ListNode *node;

	list_rewind(server->routes, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		list_delete_node(server->routes, node);
	}
}

static void delete_channels(void)
{
	ListIterator iterator;
	ListNode *node;

	list_rewind(server->channels, &iterator);
	while ((node = list_next_node(&iterator)) != NULL)
	{
		list_delete_node(server->channels, node);
	}
}

static int set_write_event(client *EagleClient)
{
	if (client->fd <= 0) {
		return EG_STATUS_ERR;
	}

	if (create_file_event(server->loop, client->fd, EG_EVENT_WRITABLE, send_response, client) == EG_EVENT_ERR &&
		EG_LIST_LENGTH(client->responses) == 0) {
		return EG_STATUS_ERR;
	}

    return EG_STATUS_OK;
}

static void send_response(EventLoop *loop, int fd, void *data, int mask)
{
	EagleClient *client = data;
	Object *object;
	int nwritten = 0, totwritten = 0;

	EG_NOTUSED(loop);
	EG_NOTUSED(mask);

	while (EG_LIST_LENGTH(client->responses))
	{
		object = EG_LIST_NODE_VALUE(EG_LIST_FIRST(client->responses));

		if (EG_OBJECT_SIZE(object) == 0) {
			list_delete_node(client->responses, EG_LIST_FIRST(client->responses));
			continue;
		}

		nwritten = write(fd, ((char*)EG_OBJECT_DATA(object)) + client->sentlen, EG_OBJECT_SIZE(object) - client->sentlen);
		if (nwritten <= 0) {
			break;
		}

		client->sentlen += nwritten;
		totwritten += nwritten;

		if (client->sentlen == EG_OBJECT_SIZE(object)) {
			list_delete_node(client->responses, EG_LIST_FIRST(client->responses));
			client->sentlen = 0;
		}
	}

	if (nwritten == -1) {
		if (errno == EAGAIN) {
			nwritten = 0;
		} else {
			free_client(client);
			return;
		}
	}

	if (totwritten > 0) {
		client->last_action = time(NULL);
	}

	if (EG_LIST_LENGTH(client->responses) == 0) {
		client->sentlen = 0;
		delete_file_event(server->loop, client->fd, EG_EVENT_WRITABLE);
	}
}


static void add_response(EagleClient *client, void *data, int size)
{
	Object *object = create_object(data, size);

	if (set_write_event(client) != EG_STATUS_OK) {
		release_object(object);
		return;
	}

	list_add_value_tail(client->responses, object);
	static inline void parse_command(EagleClient *client, ProtocolRequestHeader* req)
	{
		commandHandler *handler = commands[req->cmd];

		if (handler) {
			client->noack = req->noack;
			handler(client);
		} else {
			add_status_response(client, req->cmd, EG_PROTOCOL_STATUS_ERROR_COMMAND);
		}
	}

}

static void add_object_response(EagleClient *client, Object *object)
{
	increment_references_count(object);

	if (set_write_event(client) != EG_STATUS_OK) {
		decrement_references_count(object);
		return;
	}

	list_add_value_tail(client->responses, object);
}

static void add_status_response(EagleClient *client, int cmd, int status)
{
	ProtocolResponseHeader *res;

	if (!client->noack)
	{
		res = (ProtocolResponseHeader*)xmalloc(sizeof(*res));

		set_response_header(res, cmd, status, 0);

		add_response(client, res, sizeof(*res));
	}
}
