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
	lastUsedProcess     view.Process
	view                view.CurrentView
	getFurtherViewsFunc GetViewFunc
}

type GetViewFunc func() (*view.View, error)

// New returns a new Client with initialView.
func New(getInitialViewFunc GetViewFunc, getFurtherViewsFunc GetViewFunc, associatedProcessPosition int) (*Client, error) {
	newClient := &Client{}
	newClient.view = view.NewCurrentView()

	initialView, err := getInitialViewFunc()
	if err != nil {
		return nil, err
	}

	newClient.view.Update(initialView)

	newClient.lastUsedProcess = initialView.GetMembers()[associatedProcessPosition]

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
	err := comm.SendRPCRequest(thisClient.lastUsedProcess, "RegisterService.Write", RegisterMsg{Value: v}, &discardResult)
	if err != nil {
		if thisClient.couldGetNewView() {
			return thisClient.Write(v)
		} else {
			return err
		}
	}
	return nil
}

// Read executes the quorum read protocol.
func (thisClient *Client) Read() (interface{}, error) {
	registerMsg := RegisterMsg{}
	err := comm.SendRPCRequest(thisClient.lastUsedProcess, "RegisterService.Read", struct{}{}, &registerMsg)
	if err != nil {
		if thisClient.couldGetNewView() {
			return thisClient.Read()
		} else {
			return registerMsg.Value, err
		}
	}
	return registerMsg.Value, err
}

type RegisterMsg struct {
	Value interface{} // Value of the register
}

func init() { gob.Register(new(RegisterMsg)) }

func (thisClient *Client) couldGetNewView() bool {
	view, err := thisClient.getFurtherViewsFunc()
	if err != nil {
		return false
	}

	if view.LessUpdatedThan(thisClient.View()) || view.Equal(thisClient.View()) {
		return false
	}

	thisClient.updateCurrentView(view)
	return true
}
