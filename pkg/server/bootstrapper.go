package server

import (
	"log"
	"net"
	"net/rpc"

	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
	// TODO create stats module to count faults, faults masked, errors, failures, ...
)

var (
	listener    net.Listener
	thisProcess view.Process

	// currentView of this server cluster.
	currentView = view.NewCurrentView()
)

func Run(bindAddr string, initialView *view.View) {
	// init global variables
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on address:", listener.Addr())

	thisProcess = view.Process{listener.Addr().String()}

	currentView.Update(initialView)

	// Enable operations or join View
	if currentView.View().HasMember(thisProcess) {
		register.mu.Unlock() // Enable r/w operations
	} else {
		// try to update currentView
		getCurrentView(currentView.View().GetMembers()...)

		if currentView.View().HasMember(thisProcess) {
			register.mu.Unlock() // Enable r/w operations
		} else {
			// join the view
			log.Println("asking to join view")
			go Join()
		}
	}

	// Accept connections forever
	rpc.Accept(listener)
}

// GetCurrentView asks processes for the its current view and returns it.
func getCurrentView(processes ...view.Process) {
	for _, loopProcess := range processes {
		var receivedView *view.View
		err := comm.SendRPCRequest(loopProcess, "RegisterService.GetCurrentView", 0, &receivedView)
		if err != nil {
			log.Println("getCurrentView:", err)
			continue
		}

		currentView.Update(receivedView)
		return
	}

	log.Fatalln("Failed to get current view from processes")
}
