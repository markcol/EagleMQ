package main

import (
	"eaglemq"
	"flag"
	"fmt"
	"os"
	"os/signal"
)

var Usage = func() {
	fmt.Printf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	if len(server.config) > 0 && config_load(server.config) != EG_STATUS_OK {
		fatal("Error load config file %s", server.config)
	}

	server = eaglemq.NewServer()
	server.Startup()

	// Capture SIGTERM to initiate an orderly shutdown.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Terminate)
	go func() {
		for sig := range c {
			warning("Received SIGTERM...")
			// TODO(markcol) this should be handled by passing a value on a
			// channel.
			server.ShutdownChannel <- 1
		}
	}()

	// TODO(markcol) abstract these away to minimize cohesion
	eaglemq.start_main_loop(server.loop)
	eaglemq.delete_time_event(server.loop, server.ufd)
	eaglemq.delete_event_loop(server.loop)

	server.Shutdown()
	os.Exit(0)
}
