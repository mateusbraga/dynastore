/*
dynastore_client is a sample implementation of a dynastore client.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/mateusbraga/dynastore/pkg/client"
	"github.com/mateusbraga/dynastore/pkg/view"
)

var (
	nTotal            = flag.Uint64("n", math.MaxUint64, "number of times to perform a read and write operation")
	sleepDuration     = flag.Duration("sleep", 0, "How long to wait between pairs of read and write")
	associatedProcess = flag.String("process", ":5000", "Main process to perform client's requests")
)

func main() {
	flag.Parse()

	//rand.Seed(time.Now().UnixNano())
	//processPosition := rand.Intn(3)
	//log.Println(processPosition)

	dynastoreClient, err := client.New(view.Process{*associatedProcess})
	if err != nil {
		log.Fatalln("FATAL:", err)
	}

	var finalValue interface{}
	for i := uint64(0); i < *nTotal; i++ {
		startRead := time.Now()
		finalValue, err = dynastoreClient.Read()
		endRead := time.Now()
		if err != nil {
			log.Fatalln(err)
		}

		startWrite := time.Now()
		err = dynastoreClient.Write(i)
		endWrite := time.Now()
		if err != nil {
			log.Fatalln(err)
		}

		if i%1000 == 0 {
			fmt.Printf("%v: Read %v (%v)-> Write (%v)\n", i, finalValue, endRead.Sub(startRead), endWrite.Sub(startWrite))
		} else {
			fmt.Printf(".")
		}

		time.Sleep(*sleepDuration)
	}
}
