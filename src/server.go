package eaglemq

import (
	"container/list"
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	EAGLE_VERSION_MAJOR = 1
	EAGLE_VERSION_MINOR = 3
	EAGLE_VERSION_PATCH = 0
	EAGLE_VERSION       = fmt.Sprintf("%d.%d.%d", EAGLE_VERSION_MAJOR, EAGLE_VERSION_MINOR, EAGLE_VERSION_PATCH)

	ascii_logo = "EagleMQ\n" +
		"Version:   %s\n" +
		"Host:      %s\n" +
		"Port:      %d\n" +
		"Event API: %s\n"

	EG_STATUS_OK  = 0
	EG_STATUS_ERR = -1

	EG_BUF_SIZE      = 32768
	EG_MAX_BUF_SIZE  = 2147483647
	EG_MAX_MSG_COUNT = 4294967295
	EG_MAX_MSG_SIZE  = 2147483647

	EG_MEMORY_CHECK_TIMEOUT = 10
)

// Command line flags
var (
	default_addr           = flag.String("addr", "127.0.0.1", "The server address.")
	default_port           = flag.Int("port", 7851, "The server port.")
	default_admin_name     = flag.String("admin-name", "eagle", "The administrator user.")
	default_admin_password = flag.String("admin-password", "eagle", "The administrator password.")
	default_pid_file       = flag.String("pid-file", "", "Path to the PID file.")
	default_log_file       = flag.String("log-file", "eaglemq.log", "Path the log file.")
	default_storage_path   = flag.String("storage-file", "eaglemq.dat", "Path to the storage file.")
	default_max_clients    = flag.Int("max_clients", 16384, "Maximum number of client connections.")
	default_max_memory     = flag.Int("max-memory", 0, "Maximum memory usage limit.")
	default_save_timeout   = flag.Int("", 0, "Timeout to save data to the storage.")
	default_client_timeout = flag.Int("", 0, "Timeout to kill non-active clients.")
	default_config_path    = flag.String("config-path", "eaglemq.conf", "Path to the configuration file.")
)

const (
	NET_ERR_LEN = 512
)

type EventLoop struct {
	loop int
}

type pid_t uint32
type mode_t uint32

type EagleServer struct {
	addr            string
	channels        *list.List
	client_timeout  int
	clients         *list.List
	config          string
	daemonize       bool
	error           [NET_ERR_LEN]string
	fd              int
	last_memcheck   time.Time
	last_save       time.Time
	logfile         string
	loop            *EventLoop
	max_clients     int
	max_memory      uint64
	msg_count       int
	name            string
	nomemory        int
	now_time        time.Time
	now_timems      time.Time
	password        string
	pidfile         string
	port            int
	queues          *list.List
	routes          *list.List
	sfd             int
	shutdown        int
	start_time      time.Time
	storage         string
	storage_timeout int
	ufd             int64
	unix_perm       mode_t
	users           *list.List

	ShutdownChannel chan<- int
	storageTicker   *time.Ticker
	memoryTicker    *time.Ticker
	clientTicker    *time.Ticker
}

// NewServer creates a new server instance, with default values set appropriately.
func NewServer() *EagleServer {
	s := &EagleServer{
		addr:            default_addr,
		port:            default_port,
		name:            default_admin_name,
		password:        default_admin_password,
		max_clients:     default_max_clients,
		max_memory:      default_max_memory,
		client_timeout:  default_client_timeout,
		storage_timeout: default_save_timeout,
		clients:         list_create(),
		users:           list_create(),
		queues:          list_create(),
		routes:          list_create(),
		channels:        list_create(),
		now_time:        time.Now(),
		now_timems:      mstime(),
		start_time:      time.Now(),
		last_save:       time.Now(),
		last_memcheck:   time.Now(),
		storage:         default_storage_path,
		logfile:         default_log_path,
		config:          default_config_path,
	}

	EG_LIST_SET_FREE_METHOD(s.users, free_user_list_handler)
	EG_LIST_SET_FREE_METHOD(s.queues, free_queue_list_handler)
	EG_LIST_SET_FREE_METHOD(s.routes, free_route_list_handler)
	EG_LIST_SET_FREE_METHOD(s.channels, free_channel_list_handler)

	return s
}

// Start starts the server allowing clients to connect to it.
func (s *EagleServer) Start() {
	fmt.Printf(ascii_logo, EAGLE_VERSION, s.addr, s.port, get_event_api_name())

	if s.pidfile {
		create_pid_file(s.pidfile)
	}

	if s.logfile {
		enable_log(s.logfile)
	}

	if s.port != 0 {
		s.fd = net_tcp_server(s.error, s.addr, s.port)
		if s.fd == EG_NET_ERR {
			fatal("Error create server: %s\n%s", s.addr, s.error)
		}
	}

	if s.fd < 0 && s.sfd < 0 {
		fatal("Error configure server")
	}

	if s.fd > 0 && create_file_event(s.loop, s.fd, EG_EVENT_READABLE, accept_tcp_handler, nil) == EG_EVENT_ERR {
		fatal("Error create file event")
	}

	if s.sfd > 0 && create_file_event(s.loop, s.sfd, EG_EVENT_READABLE, accept_unix_handler, nil) == EG_EVENT_ERR {
		fatal("Error create file event")
	}

	s.initAdmin()
	s.initStorage()
	wlog("Server started (version: %s)", EAGLE_VERSION)

	// Create timers for periodic actions if they have non-zero values
	if s.storage_timeout > 0 {
		s.storageTicker = time.NewTicker(s.storage_timeout * time.Second)
	}
	if s.memory_timeout > 0 {
		s.memoryTicker = time.NewTicker(s.memory_timeout * time.Second)
	}
	if s.client_timeout > 0 {
		s.clientTicker = time.NewTicker(s.client_timeout * time.Second)
	}
	for {
		select {
		case <-s.storageTicker.C:
			s.storageTimeout()
		case <-s.memoryTicker.C:
			s.memoryTimeout()
		case <-s.clientTicker.C:
			s.clientTimeout()
		case <-s.ShutdownChannel:
			s.memoryTicker.Stop()
			s.storageTicker.Stop()
			s.clientTicker.Stop()
		}
	}
}

// Shutdown terminates the server and cleans up any transient resources.
func (s *EagleServer) Terminate() {
	if s.fd > 0 {
		s.fd.Close(s.fd)
	}
	if s.sfd > 0 {
		s.sfd.Close(s.sfd)
	}
	if len(s.pidfile) > 0 {
		os.Unlink(s.pidfile)
	}
	disable_log()
}

// create_pid_file
func create_pid_file(pidfile string) {
	fp, err := os.Open(pidfile)
	if err != nil {
		return err
	}
	defer fp.Close()
	fp.WriteString(fmt.Sprintf("%d\n", getpid()))
}

// unblock_open_files_limit
func unblock_open_files_limit() {
	var (
		maxfiles = server.max_clients + 32
		limit    rlimit
	)

	if maxfiles < 1024 {
		maxfiles = 1024
	}

	if getrlimit(RLIMIT_NOFILE, &limit) == -1 {
		warning("Error getrlimit", strerror(errno))
		server.max_clients = 1024 - 32
	} else {
		oldlimit := limit.rlim_cur

		if oldlimit < maxfiles {
			limit.rlim_cur = maxfiles
			limit.rlim_max = maxfiles

			if setrlimit(RLIMIT_NOFILE, &limit) == -1 {
				server.max_clients = oldlimit - 32
			}
		}
	}
}

// process_queues_messages
func process_queues_messages() {
	var iterator ListIterator

	list_rewind(server.queues, &iterator)
	for node := list_next_node(&iterator); node != nil; node = list_next_node(&iterator) {
		queue_t := EG_LIST_NODE_VALUE(node)
		process_expired_messages_queue_t(queue_t, server.now_timems)
		process_unconfirmed_messages_queue_t(queue_t, server.now_timems)
	}
}

// memoryTimeout handle the period memory timeout ticker.
func (s *EagleServer) memoryTimeout() {
	if xmalloc_used_memory() > s.max_memory {
		warning("Used memory: %u, limit: %u", xmalloc_used_memory(), s.max_memory)
		s.last_memcheck = time.Now()
		s.nomemory = 1
	} else {
		s.nomemory = 0
	}
}

// storageTimeout flushes the storage to disk each time it is called.
func (s *EagleServer) storageTimeout() {
	wlog("Save all data to the storage...")
	if storage_save_background(s.storage) != EG_STATUS_OK {
		warning("Error saving data in %s", s.storage)
	}
	s.last_save = time.Now()
}

// clientTimeout closes all clients that have been inactive for more than
// client_timeout seconds each time it is called.
func (s *EagleServer) clientTimeout() {
	var iterator ListIterator

	list_rewind(s.clients, &iterator)
	for node := list_next_node(&iterator); node != nil; node = list_next_node(&iterator) {
		client := EG_LIST_NODE_VALUE(node)
		if (time.Now() - client.last_action) > s.client_timeout {
			free_client(client)
		}
	}
}

// server_updater
// TODO(markcol): should data be a byte slice?
func server_updater(loop *EventLoop, id int64, data interface{}) int {
	server.now_time = time(nil)
	server.now_timems = mstime()
	server.msg_counter = 0

	process_queues_messages()

	return 100
}

// initAdmin creates the initial admin user for the server
func (s *EagleServer) initAdmin() {
	admin := create_user(s.name, s.password, EG_USER_SUPER_PERM)
	list_add_value_tail(s.users, admin)
}

// initStorage initialize the storage file.
func (s *EagleServer) initStorage() {
	if storage_save(s.storage) != EG_STATUS_OK {
		warning("Error init storage %s\n", s.storage)
	}

	if storage_load(s.storage) != EG_STATUS_OK {
		warning("Error loading data from %s", s.storage)
	}
}
