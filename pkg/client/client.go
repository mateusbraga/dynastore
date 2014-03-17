/*
Package client implements a dynastore client
*/
package client

import (
	"encoding/gob"
	"log"
	"math/rand"
	"time"

	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
)

type Client struct {
	lastUsedProcess     view.Process
	view                view.CurrentView
	getFurtherViewsFunc GetViewFunc
}

type GetViewFunc func() (*view.View, error)

// New returns a new Client with initialView.
func New(getInitialViewFunc GetViewFunc, getFurtherViewsFunc GetViewFunc) (*Client, error) {
	newClient := &Client{}
	newClient.view = view.NewCurrentView()

	initialView, err := getInitialViewFunc()
	if err != nil {
		return nil, err
	}

	newClient.view.Update(initialView)

	rand.Seed(time.Now().UnixNano())
	processPosition := rand.Intn(initialView.NumberOfMembers())
	log.Println(initialView.NumberOfMembers(), processPosition)
	newClient.lastUsedProcess = initialView.GetMembers()[processPosition]

	newClient.getFurtherViewsFunc = getFurtherViewsFunc

	return newClient, nil
}

func (thisClient Client) View() *view.View      { return thisClient.view.View() }
func (thisClient Client) ViewRef() view.ViewRef { return thisClient.view.ViewRef() }
func (thisClient Client) ViewAndViewRef() (*view.View, view.ViewRef) {
	return thisClient.view.ViewAndViewRef()
}
func (thisClient *Client) updateCurrentView(newView *view.View) { thisClient.view.Update(newView) }

// Write v to the system's register. Can be run concurrently.
func (thisClient *Client) Write(v interface{}) error {
	discardResult := struct{}{}
	return comm.SendRPCRequest(thisClient.lastUsedProcess, "RegisterService.Write", RegisterMsg{Value: v}, &discardResult)
}

// Read executes the quorum read protocol.
func (thisClient *Client) Read() (interface{}, error) {
	registerMsg := RegisterMsg{}
	err := comm.SendRPCRequest(thisClient.lastUsedProcess, "RegisterService.Read", struct{}{}, &registerMsg)
	return registerMsg.Value, err
}

type RegisterMsg struct {
	Value interface{} // Value of the register
}

func init() { gob.Register(new(RegisterMsg)) }
