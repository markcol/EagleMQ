package eaglemq

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TODO(markcol): attach methods to global configuration object.
// TODO(markcol): change the API to return error values per Go idiom.
// TODO(markcol): convert from custom logging to Go logging package
// TODO(markcol): convert to use of Go LPZ compression library
// TODO(markcol): split into serving program and API library

// config_parse_key_value parses a (key, value) pair, checks for proper value
// semantics, and assigns the value to the global configuration object of the
// same key name.
func config_parse_key_value(key, value string) int {
	switch key {
	case "addr":
		server.addr = value
	case "port":
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return EG_STATUS_ERR
		}
		server.port = int(v)
	case "unix-socket":
		server.unix_socket = value
	case "admin-name":
		server.name = value
	case "admin-password":
		server.password = value
	case "daemonize":
		// TODO(markcol): Go programs don't daemonize--remove or log a warning
		server.daemonize = isOn(value)
	case "pid-file":
		server.pidfile = value
	case "log-file":
		server.logfile = value
	case "storage-file":
		server.storage = value
	case "max-clients":
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return EG_STATUS_ERR
		}
		server.max_clients = int(v)
	case "max-memory":
		// TODO(markcol): Fix this to use Go memory counters properly
		// max_memory := memtoll(value, &err)
		// if err != nil {
		//	return EG_STATUS_ERR
		// }

		max_memory := uint64(2*1024 ^ 3)
		server.max_memory = max_memory
	case "save-timeout":
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return EG_STATUS_ERR
		}
		server.storage_timeout = int(v)
	case "client-timeout":
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return EG_STATUS_ERR
		}
		server.client_timeout = int(v)
	default:
		// TODO(markcol): fix to use Go logging
		// info("Error parse key: %s", key)
		return EG_STATUS_ERR
	}
	return EG_STATUS_OK
}

// config_load read and parse the configuration values, setting the global
// configuration object values to match the settings in the file.
//
// TODO(markcol): change to return an error rather than an int.
func config_load(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return EG_STATUS_ERR
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := parseConfigLine(scanner.Text()); err != nil {
			return EG_STATUS_ERR
		}
	}
	return EG_STATUS_OK
}

// Private functions below this point

// isOn returns true if the input value is "on" or "true". Comparison
// is case insensitive.
func isOn(value string) bool {
	value = strings.ToLower(value)
	if value == "on" || value == "true" {
		return true
	}
	return false
}

// parseConfigLine parses a single configuration file line. Valid line
// syntax is an empty line, a comment starting with '#', or a key-value pair
// optionally followed by a comment.
func parseConfigLine(line string) error {
	// Check for comment; if found, remove all characters to end of line
	i := strings.IndexByte(line, '#')
	if i >= 0 {
		line = line[:i]
	}

	// remove any leading or trailing whitespace
	line = strings.TrimSpace(line)

	// if line is empty (newline, comment, empty), ignore it
	if len(line) == 0 {
		fmt.Printf("found empty line")
		return nil
	}

	// parse into key, value pair.
	toks := strings.Fields(line)
	if len(toks) == 2 {
		fmt.Printf("%q: %q", toks[0], toks[1])
		return nil
		if config_parse_key_value(toks[0], toks[1]) == EG_STATUS_OK {
			return errors.New("Invalid key-value syntax")
		}
	}
	return errors.New("Invalid configuration line syntax")
}
