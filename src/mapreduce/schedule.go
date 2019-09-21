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
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	// 要等待所有的任务完成后，才能结束这个函数，所以，添加 wg
	var wg sync.WaitGroup
	taskchan:=make(chan int, 0)
	go func(){
		for i:=0; i< ntasks;i++{
			taskchan <- i
		}
	}()
	wg.Add(ntasks)
	go func(){
		for{
			service := <-registerChan
			i := <-taskchan
			args := DoTaskArgs{
				JobName:jobName,
				Phase:phase,
				TaskNumber:i,
				NumOtherPhase:n_other,
			}
			if phase == mapPhase{
				args.File = mapFiles[i]
			}
			go func(){
				if call(service, "Worker.DoTask",args,nil){
					wg.Done()
					//任务结束后,这项服务传回给"可注册的任务"
					registerChan <- service
				}else{
					//把没有完成的任务传回给taskchan
					taskchan <- args.TaskNumber
				}

			}()
		}
	}()

	//阻塞
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
