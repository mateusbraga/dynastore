/*
Package client implements a dynastore client
*/
package client

import (
	"encoding/gob"

	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
)

type Client struct {
	associatedServer view.Process
}

// New returns a new Client with initialView.
func New(associatedServer view.Process) (*Client, error) {
	newClient := &Client{}
	newClient.associatedServer = associatedServer

	return newClient, nil
}

// Write v to the system's register. Can be run concurrently.
func (thisClient *Client) Write(v interface{}) error {
	discardResult := struct{}{}
	return comm.SendRPCRequest(thisClient.associatedServer, "RegisterService.Write", RegisterMsg{Value: v}, &discardResult)
}

// Read executes the quorum read protocol.
func (thisClient *Client) Read() (interface{}, error) {
	registerMsg := RegisterMsg{}
	err := comm.SendRPCRequest(thisClient.associatedServer, "RegisterService.Read", struct{}{}, &registerMsg)
	return registerMsg.Value, err
}

type RegisterMsg struct {
	Value interface{} // Value of the register
}

func init() { gob.Register(new(RegisterMsg)) }
