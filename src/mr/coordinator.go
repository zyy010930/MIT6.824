package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	JobChannelMap        chan *Job
	JobChannelReduce     chan *Job
	jobId                int
	MapNum               int
	ReduceNum            int
	CoordinatorCondition Condition
	jobMetaHolder        JobMetaHolder
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		ReduceNum:        nReduce,
		MapNum:           len(files),
		jobId:            0,
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		CoordinatorCondition: MapPhase,
	}

	c.makeMapJob(files)

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) getJobId() int {
	res := c.jobId
	c.jobId++
	return res
}

type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReducerNum int
}

type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

func (j *JobMetaHolder) putJob(jobInfo *JobMetaInfo) bool {
	id := jobInfo.JobPtr.JobId
	isEmpty := j.MetaMap[id]
	if isEmpty != nil {
		fmt.Printf("Job is already exist")
		return false
	} else {
		j.MetaMap[id] = jobInfo
	}
	return true
}

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}

func (c *Coordinator) makeMapJob(files []string) {
	for _, v := range files {
		id := c.getJobId()
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReduceNum,
		}
		jobMetaINfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaINfo)
		c.JobChannelMap <- &job
		fmt.Println("making map job :", &job)
	}
}

func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo) {
	res, ok := j.MetaMap[jobId]
	return ok, res
}

func (j *JobMetaHolder) fireTheJob(id int) bool {
	flag, jobInfo := j.getJobMetaInfo(id)
	if !flag || jobInfo.condition != JobWaiting {
		return false
	} else {
		jobInfo.condition = JobWorking
		jobInfo.StartTime = time.Now()
		return true
	}
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		//c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		//close(c.JobChannelReduce)
		c.CoordinatorCondition = AllDone
	}
}

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("coordinator get a request from worker :")
	if c.CoordinatorCondition == MapPhase {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Printf("job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else {
		reply.JobType = KillJob
	}
	return nil

}
