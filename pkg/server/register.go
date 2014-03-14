//TODO make it a key value storage
package server

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/mateusbraga/dynastore/pkg/comm"
	"github.com/mateusbraga/dynastore/pkg/view"
	"github.com/mateusbraga/dynastore/pkg/weaksnapshot"
)

var register Value
var maxRegister Value
var pickNewTimestamp bool

//  ---------- RPC Requests -------------
type RegisterService int

func init() { rpc.Register(new(RegisterService)) }

func (r *RegisterService) Read(clientViewRef view.ViewRef, reply *Value) error {
	register.mu.Lock()
	defer register.mu.Unlock()

	pickNewTimestamp = false
	newView := traverse(nil, Value{})
	if newView != nil {
		notifyQ(newView)
	}

	reply.Value = maxRegister.Value
	reply.Timestamp = maxRegister.Timestamp

	throughput++

	return nil
}

func (r *RegisterService) Write(value Value, reply *Value) error {
	register.mu.Lock()
	defer register.mu.Unlock()

	pickNewTimestamp = true
	newView := traverse(nil, value)
	if newView != nil {
		notifyQ(newView)
	}

	throughput++

	return nil
}

// ----- Traverse

func traverse(changes []view.Update, value Value) *view.View {
	desiredView := view.NewWithUpdates(append(currentView.View().GetUpdates(), changes...)...)
	front := ViewSeq{currentView.View()}

	var changeSets [][]view.Update
	for {
		leastUpdatedView := front.GetLeastUpdatedView()
		if !leastUpdatedView.HasMember(thisProcess) {
			log.Println("Halting...")
			os.Exit(0)
		}

		if !leastUpdatedView.Equal(desiredView) {
			weaksnapshot.Update(leastUpdatedView, thisProcess, desiredView.GetUpdatesNotIn(leastUpdatedView))
		}

		changeSets = readInView(leastUpdatedView)
		if len(changeSets) != 0 {
			front = front.Delete(leastUpdatedView)
			allUpdates := []view.Update{}
			for _, updates := range changeSets {
				allUpdates = append(allUpdates, updates...)
				front = append(front, leastUpdatedView.NewCopyWithUpdates(updates...))
			}
			desiredView = desiredView.NewCopyWithUpdates(allUpdates...)
		} else {
			changeSets = writeInView(leastUpdatedView, value)
		}

		if len(changeSets) == 0 {
			break
		}
	}
	currentView.Update(desiredView)
	return desiredView
}

func readInView(destinationView *view.View) [][]view.Update {
	changeSets := weaksnapshot.Scan(destinationView, thisProcess)
	contactQ("R", destinationView)
	return changeSets
}

func writeInView(destinationView *view.View, value Value) [][]view.Update {
	if pickNewTimestamp {
		pickNewTimestamp = false
		maxRegister.Value = value.Value
		//TODO append process id
		maxRegister.Timestamp++
	}
	contactQ("W", destinationView)
	changeSets := weaksnapshot.Scan(destinationView, thisProcess)
	return changeSets
}

func contactQ(msgType string, destinationView *view.View) {
	if msgType == "W" {
		err := comm.BroadcastRPCRequest(destinationView, "ContactQService.Write", Value{Value: maxRegister.Value, Timestamp: maxRegister.Timestamp})
		if err != nil {
			log.Fatalln("Failed to get ContactQService.Write quorum")
		}
	} else {
		resultChan := make(chan Value, destinationView.NumberOfMembers())
		for _, process := range destinationView.GetMembers() {
			go sendRead(process, resultChan)
		}

		failedTotal := 0
		successTotal := 0
		var mostRecentValue Value
		for {
			result := <-resultChan
			if result.Err != nil {
				log.Println("+1 error on read:", result.Err)
				failedTotal++
				continue
			}

			successTotal++
			if result.Timestamp > mostRecentValue.Timestamp {
				mostRecentValue.Value = result.Value
				mostRecentValue.Timestamp = result.Timestamp
			}

			if successTotal == destinationView.QuorumSize() {
				if mostRecentValue.Timestamp > maxRegister.Timestamp {
					maxRegister.Timestamp = mostRecentValue.Timestamp
					maxRegister.Value = mostRecentValue.Value
				}
				return
			}
		}
	}
}

func sendRead(process view.Process, resultChan chan Value) {
	var result Value
	err := comm.SendRPCRequest(process, "ContactQService.Read", struct{}{}, &result)
	if err != nil {
		resultChan <- Value{Err: err}
		return
	}

	resultChan <- result
}

type ContactQService struct{}

func init() { rpc.Register(new(ContactQService)) }

func (nq *ContactQService) Read(arg struct{}, reply *Value) error {
	reply.Value = register.Value
	reply.Timestamp = register.Timestamp
	return nil
}

func (nq *ContactQService) Write(value Value, reply *struct{}) error {
	if value.Timestamp > register.Timestamp {
		register.Timestamp = value.Timestamp
		register.Value = value.Value
	}
	return nil
}

// ----- NotifyQ

func notifyQ(newView *view.View) {
	receivedViewListMu.Lock()
	defer receivedViewListMu.Unlock()

	i, viewCounter := findNotifyViewCounter(NotifyMsg{View: newView, Sender: thisProcess})

	<-viewCounter.gotQuorumC
}

type viewCounter struct {
	view       *view.View
	received   map[view.Process]bool
	gotQuorumC chan struct{}
}

var receivedViewList []viewCounter
var receivedViewListMu sync.Mutex

func findNotifyViewCounter(notifyMsg NotifyMsg) (int, viewCounter) {
	var gotQuorumC *chan struct{}

	for i, loopViewCounter := range receivedViewList {
		if loopViewCounter.view.Equal(notifyMsg.View) {
			return i, loopViewCounter
		}
	}

	// if not found, resend and create it

	comm.BroadcastRPCRequest(notifyMsg.View, "NotifyQService.Notify", notifyMsg)

	newViewCounter := viewCounter{view: notifyMsg.View, received: make(map[view.Process]bool), gotQuorumC: make(chan struct{})}
	receivedViewList = append(receivedViewList, newViewCounter)
	return len(receivedViewList) - 1, newViewCounter
}

type NotifyQService struct{}

func init() { rpc.Register(new(NotifyQService)) }

type NotifyMsg struct {
	View   *view.View
	Sender view.Process
}

func (nq *NotifyQService) Notify(notifyMsg NotifyMsg, reply *struct{}) error {
	receivedViewListMu.Lock()
	defer receivedViewListMu.Unlock()

	i, viewCounter := findNotifyViewCounter(notifyMsg)

	viewCounter.received[notifyMsg.Sender] = true
	receivedViewList[i] = viewCounter

	if len(viewCounter.received) == viewCounter.view.QuorumSize() || viewCounter.view.QuorumSize() == 0 {
		close(viewCounter.gotQuorumC)
	}
	return nil
}

//func (r *RegisterService) GetCurrentView(value int, reply *view.View) error {
//*reply = *currentView.View()
//log.Println("Done GetCurrentView request")
//return nil
//}

// --------- Init ---------
func init() {
	register.mu.Lock() // The register starts locked
	register.Value = nil
	register.Timestamp = 0
}

// --------- Types ---------
type Value struct {
	Value     interface{}
	Timestamp int

	ViewRef view.ViewRef
	Err     error

	mu sync.RWMutex
}

var throughput uint64
var throughputBuffer = make(map[time.Time]uint64, 70)
var shutdownChan = make(chan bool)

func collectThroughputWorker() {
	writeLength := rand.Intn(20)
	var lastThroughput uint64

	ticker := time.Tick(time.Second)
	for {
		select {
		case now := <-ticker:
			aux := throughput
			throughputBuffer[now] = aux - lastThroughput
			lastThroughput = aux

			if len(throughputBuffer) > writeLength {
				writeLength = rand.Intn(20)
				saveThroughput()
			}
		case _ = <-shutdownChan:
			saveThroughput()
			log.Fatalln("Terminated")
		}
	}
	log.Println("STOPPED COLLECTING THROUGHPUT!")
}

func saveThroughput() {
	filename := fmt.Sprintf("/proj/freestore/throughputs_%v.txt", currentView.View().GetProcessPosition(thisProcess))
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		//log.Println(err)
		for loopTime, loopThroughput := range throughputBuffer {
			fmt.Printf("%v %v %v\n", thisProcess, loopThroughput, loopTime.Format(time.RFC3339))
			delete(throughputBuffer, loopTime)
		}
		return
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	for loopTime, loopThroughput := range throughputBuffer {
		if _, err = w.Write([]byte(fmt.Sprintf("%v %v %v\n", thisProcess, loopThroughput, loopTime.Format(time.RFC3339)))); err != nil {
			log.Fatalln(err)
		}
		delete(throughputBuffer, loopTime)
	}
}

func init() {
	go collectThroughputWorker()
	rand.Seed(int64(time.Now().Nanosecond()))
}
