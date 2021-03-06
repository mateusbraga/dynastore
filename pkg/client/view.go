package client

import (
	"errors"
	"fmt"
	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
	"log"
)

// GetCurrentView asks processes for the its current view and returns it.
func GetCurrentView(processes ...view.Process) (*view.View, error) {
	for _, loopProcess := range processes {
		var receivedView *view.View
		err := comm.SendRPCRequest(loopProcess, "RegisterService.GetCurrentView", 0, &receivedView)
		if err != nil {
			log.Println("GetCurrentView:", err)
			continue
		}

		return receivedView, nil
	}
	return nil, errors.New(fmt.Sprintf("Failed to get current view from any of the processes: %v", processes))
}
