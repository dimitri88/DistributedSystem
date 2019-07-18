package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	prod := make(chan string, ntasks)
	quit := make(chan interface{})

	go func() {
		for {
			select {
			case workerAddress := <-registerChan:
				prod <- workerAddress
			case <-quit:
				close(prod)
				return
			}
		}
	}()

	for i := 0; i < ntasks; i++ {

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				workerAddress := <-prod
				testArgs := DoTaskArgs{
					JobName:       jobName,
					File:          mapFiles[i],
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: nOther,
				}
				success := call(workerAddress, "Worker.DoTask", testArgs, nil)
				prod <- workerAddress
				if success {
					return
				}
			}

		}(i)
	}
	wg.Wait()
	close(quit)
	fmt.Printf("Schedule: %v done\n", phase)
}
