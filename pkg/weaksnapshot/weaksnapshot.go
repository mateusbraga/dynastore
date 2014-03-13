package weaksnapshot

import (
	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
	"log"
	"net/rpc"
	"sync"
)

var (
	weakSnapshotList   []weakSnapshot
	weakSnapshotListMu sync.Mutex
)

type weakSnapshot struct {
	associatedView    *view.View
	associatedProcess view.Process
	registers         []register
}

func (ws *weakSnapshot) collect() []interface{} {
	var result []interface{}

	registerValueChan := make(chan interface{}, 20)
	for registerIndex := 0; registerIndex < ws.associatedView.NumberOfMembers(); registerIndex++ {
		go ws.readQuorum(registerIndex, registerValueChan)
	}

	totalReceived := 0
	for registerValue := range registerValueChan {
		if registerValue != nil {
			result = append(result, registerValue)
		}
		totalReceived++
		if totalReceived == ws.associatedView.NumberOfMembers() {
			break
		}
	}

	if len(result) == 0 {
		return nil
	} else {
		return result
	}
}

func (ws *weakSnapshot) write(registerIndex int, change interface{}) {
	ws.registers[registerIndex].value = change
	ts := ws.registers[registerIndex].timestamp
	ts++
	ws.registers[registerIndex].timestamp = ts
}

func (ws *weakSnapshot) readQuorum(registerIndex int, returnChan chan<- interface{}) {
	readMsg := registerMsg{}
	readMsg.RegisterIndex = registerIndex
	readMsg.View = ws.associatedView

	// Send write request to all
	resultChan := make(chan registerMsg, ws.associatedView.NumberOfMembers())
	go broadcastRead(ws.associatedView, readMsg, resultChan)

	// Wait for quorum
	var failedTotal int
	var successTotal int
	for {
		receivedValue := <-resultChan

		// count success or failure
		if receivedValue.Err != nil {
			log.Println("+1 error to read:", receivedValue.Err)
			failedTotal++
			if failedTotal > ws.associatedView.NumberOfToleratedFaults() {
				log.Fatalln("weaksnapshot: Failed to get read quorum")
			}
		} else {
			returnChan <- receivedValue
			successTotal++
		}
	}
}

func Scan(associatedView *view.View, thisProcess view.Process) []interface{} {
	ws := getOrCreateWeakSnapshot(associatedView, thisProcess)

	result := ws.collect()
	if result == nil {
		return nil
	}

	return ws.collect()
}

func Update(associatedView *view.View, thisProcess view.Process, change interface{}) {
	ws := getOrCreateWeakSnapshot(associatedView, thisProcess)

	result := ws.collect()
	if result == nil {
		registerIndex := associatedView.GetProcessPosition(thisProcess)
		ws.write(registerIndex, change)
	}
}

func getOrCreateWeakSnapshot(associatedView *view.View, thisProcess view.Process) weakSnapshot {
	weakSnapshotListMu.Lock()
	defer weakSnapshotListMu.Unlock()

	for _, loopWs := range weakSnapshotList {
		if loopWs.associatedView.Equal(associatedView) && loopWs.associatedProcess == thisProcess {
			return loopWs
		}
	}

	// create new one
	newWs := weakSnapshot{associatedView: associatedView, associatedProcess: thisProcess}
	newRegisterList := make([]register, associatedView.NumberOfMembers())
	newWs.registers = newRegisterList

	weakSnapshotList = append(weakSnapshotList, newWs)

	return newWs
}

type register struct {
	value     interface{}
	timestamp int
}

type registerMsg struct {
	Value         interface{}
	RegisterIndex int
	View          *view.View
	Process       view.Process
	Err           error
}

func sendRead(process view.Process, msg registerMsg, resultChan chan registerMsg) {
	var result registerMsg
	err := comm.SendRPCRequest(process, "WeakSnapshotService.ReadRegister", msg, &result)
	if err != nil {
		resultChan <- registerMsg{Err: err}
		return
	}

	resultChan <- result
}

func broadcastRead(destinationView *view.View, msg registerMsg, resultChan chan registerMsg) {
	for _, process := range destinationView.GetMembers() {
		go sendRead(process, msg, resultChan)
	}
}

type WeakSnapshotService struct{}

func init() { rpc.Register(WeakSnapshotService{}) }

func (wss *WeakSnapshotService) Read(readMsg registerMsg, resultMsg *registerMsg) error {
	ws := getOrCreateWeakSnapshot(readMsg.View, readMsg.Process)

	resultMsg.Value = ws.registers[readMsg.RegisterIndex].value
	return nil
}
