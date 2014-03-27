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

func init() { register.mu.Lock() }

//  ---------- RPC Requests -------------
type RegisterService int

func init() { rpc.Register(new(RegisterService)) }

func (r *RegisterService) GetCurrentView(value int, reply *view.View) error {
	*reply = *currentView.View()
	log.Println("Done GetCurrentView request")
	return nil
}

func (r *RegisterService) Read(arg struct{}, reply *Value) error {
	var thisOpValue Value
	pickNewTimestamp := false
	newView := traverse(&thisOpValue, nil, nil, &pickNewTimestamp)
	if newView != nil {
		notifyQ(newView)
	}

	reply.Value = thisOpValue.Value
	reply.Timestamp = thisOpValue.Timestamp

	return nil
}

func (r *RegisterService) Write(value Value, reply *struct{}) error {
	var thisOpValue Value
	pickNewTimestamp := true
	newView := traverse(&thisOpValue, nil, value.Value, &pickNewTimestamp)
	if newView != nil {
		notifyQ(newView)
	}

	return nil
}

// ----- reconfig

func (r *RegisterService) Reconfig(updates []view.Update, reply *struct{}) error {
	var thisOpValue Value
	pickNewTimestamp := false
	newView := traverse(&thisOpValue, updates, nil, &pickNewTimestamp)
	if newView != nil {
		notifyQ(newView)
	}
	return nil
}

func Join() {
	comm.BroadcastRPCRequest(currentView.View(), "RegisterService.Reconfig", []view.Update{view.Update{view.Join, thisProcess}})
}

func Leave() {
	comm.BroadcastRPCRequest(currentView.View(), "RegisterService.Reconfig", []view.Update{view.Update{view.Leave, thisProcess}})
}

// ----- Traverse

func traverse(thisOpValue *Value, changes []view.Update, value interface{}, pickNewTimestamp *bool) *view.View {
	initialCurrentView := currentView.View()

	var desiredView *view.View
	if changes != nil {
		desiredView = view.NewWithUpdates(append(initialCurrentView.GetUpdates(), changes...)...)
	} else {
		desiredView = initialCurrentView
	}
	front := ViewSeq{initialCurrentView}

	var changeSets [][]view.Update
	for {
		leastUpdatedView := front.GetLeastUpdatedView()
		if !leastUpdatedView.HasMember(thisProcess) {
			log.Println("Halting...")
			os.Exit(0)
		}

		if !leastUpdatedView.Equal(desiredView) {
			log.Println("desired != leastUpdatedView")
			weaksnapshot.Update(leastUpdatedView, thisProcess, desiredView.GetUpdatesNotIn(leastUpdatedView))
		}

		changeSets = readInView(leastUpdatedView, thisOpValue)
		if len(changeSets) != 0 {
			log.Println("changeSets != 0")
			front = front.Delete(leastUpdatedView)
			allUpdates := []view.Update{}
			for _, updates := range changeSets {
				allUpdates = append(allUpdates, updates...)
				front = append(front, leastUpdatedView.NewCopyWithUpdates(updates...))
			}
			desiredView = desiredView.NewCopyWithUpdates(allUpdates...)
		} else {
			//log.Println("changeSets == 0")
			changeSets = writeInView(leastUpdatedView, value, thisOpValue, pickNewTimestamp)
		}

		if len(changeSets) == 0 {
			//log.Println("changeSets == 0")
			break
		}
	}

	if !initialCurrentView.Equal(currentView.View()) {
		// restart, we got a notify request that changed the current view
		return traverse(thisOpValue, changes, value, pickNewTimestamp)
	}

	if currentView.View().LessUpdatedThan(desiredView) {
		log.Println("installing desired view")
		currentView.Update(desiredView)
		return desiredView
	} else {
		return nil
	}
}

func readInView(destinationView *view.View, thisOpValue *Value) [][]view.Update {
	changeSets := weaksnapshot.Scan(destinationView, thisProcess)
	contactQ("R", destinationView, thisOpValue)
	return changeSets
}

func writeInView(destinationView *view.View, value interface{}, thisOpValue *Value, pickNewTimestamp *bool) [][]view.Update {
	if *pickNewTimestamp {
		*pickNewTimestamp = false
		thisOpValue.Value = value
		//TODO append process id
		thisOpValue.Timestamp++
	}
	contactQ("W", destinationView, thisOpValue)
	changeSets := weaksnapshot.Scan(destinationView, thisProcess)
	return changeSets
}

func contactQ(msgType string, destinationView *view.View, thisOpValue *Value) {
	if msgType == "W" {
		err := comm.BroadcastRPCRequest(destinationView, "ContactQService.Write", *thisOpValue)
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
				//log.Println("+1 error on read:", result.Err)
				failedTotal++
				continue
			}

			successTotal++
			if result.Timestamp > mostRecentValue.Timestamp {
				mostRecentValue.Value = result.Value
				mostRecentValue.Timestamp = result.Timestamp
			}

			if successTotal == destinationView.QuorumSize() {
				if mostRecentValue.Timestamp > thisOpValue.Timestamp {
					thisOpValue.Timestamp = mostRecentValue.Timestamp
					thisOpValue.Value = mostRecentValue.Value
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
	//log.Println("ContactQService.Read")

	register.mu.RLock()
	defer register.mu.RUnlock()

	throughput++
	reply.Value = register.Value
	reply.Timestamp = register.Timestamp
	return nil
}

func (nq *ContactQService) Write(value Value, reply *struct{}) error {
	//log.Println("ContactQService.Write")

	register.mu.Lock()
	defer register.mu.Unlock()

	if value.Timestamp > register.Timestamp {
		register.Timestamp = value.Timestamp
		register.Value = value.Value
	}
	return nil
}

// ----- NotifyQ

var notifyViewCounterList []viewCounterInstance
var notifyViewCounterListMu sync.Mutex

func getOrCreateNotifyViewCounter(associatedView *view.View) viewCounterInstance {
	notifyViewCounterListMu.Lock()
	defer notifyViewCounterListMu.Unlock()

	for _, loopNvc := range notifyViewCounterList {
		if loopNvc.view.Equal(associatedView) {
			return loopNvc
		}
	}

	// Create new one, and send notify msgs
	go comm.BroadcastRPCRequest(associatedView, "NotifyQService.Notify", NotifyMsg{View: associatedView, Sender: thisProcess})

	newViewCounter := viewCounterInstance{done: make(chan struct{}), view: associatedView, gotQuorumC: make(chan struct{}), newNotifyMsgC: make(chan NotifyMsg, 20)}
	notifyViewCounterList = append(notifyViewCounterList, newViewCounter)

	go notifyViewCounterWorker(newViewCounter)

	if currentView.View().LessUpdatedThan(associatedView) {
		if !currentView.View().HasMember(thisProcess) && associatedView.HasMember(thisProcess) {
			defer func() { register.mu.Unlock() }()
		}
		currentView.Update(associatedView)
	}

	return newViewCounter
}

func notifyViewCounterWorker(vci viewCounterInstance) {
	receivedMsgs := make(map[view.Process]bool, vci.view.NumberOfMembers())
	for {
		select {
		case notifyMsg := <-vci.newNotifyMsgC:
			receivedMsgs[notifyMsg.Sender] = true

			if len(receivedMsgs) == vci.view.QuorumSize() || vci.view.QuorumSize() == 0 {
				close(vci.gotQuorumC)
			}
		case <-vci.done:
		}
	}
}

func notifyQ(newView *view.View) {
	vci := getOrCreateNotifyViewCounter(newView)
	<-vci.gotQuorumC
}

type viewCounterInstance struct {
	done          chan struct{}
	view          *view.View
	newNotifyMsgC chan NotifyMsg
	gotQuorumC    chan struct{}
}

type NotifyQService struct{}

func init() { rpc.Register(new(NotifyQService)) }

type NotifyMsg struct {
	View   *view.View
	Sender view.Process
}

func (nq *NotifyQService) Notify(notifyMsg NotifyMsg, reply *struct{}) error {
	vci := getOrCreateNotifyViewCounter(notifyMsg.View)
	vci.newNotifyMsgC <- notifyMsg
	return nil
}

// --------- Types ---------
type Value struct {
	Value     interface{}
	Timestamp int

	Err error
	mu  sync.RWMutex
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
