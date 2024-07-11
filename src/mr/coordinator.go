package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	StageMap = iota
	StageReduce
	StageEnd
)

const TimeoutLimit = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	NReduce     int    // 分片结果限制
	MapTasks    []Task // MapTasks与Files一一对应
	ReduceTasks []Task // 由分片数量(NReduce)决定

	Stage int        // 分为Map(起始)、Reduce(Map结束)、End(Reduce结束)
	Mutex sync.Mutex // 互斥锁
}

type Task struct {
	Pid            int
	TaskInput      string    // 对于Map，放入对应File
	TaskStatus     Status    // Ready、Done or Handling
	TaskAssignedAt time.Time // 超时处理
}

func (c *Coordinator) init(filenames []string) {
	for i, filename := range filenames {
		c.MapTasks[i] = Task{
			TaskInput:      filename,
			TaskStatus:     Ready,
			TaskAssignedAt: time.Time{},
		}
	}
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = Task{
			TaskStatus: Ready,
		}
	}
}

func (c *Coordinator) Assignable(task *Task) bool {
	return task.TaskStatus == Ready || (task.TaskStatus == Handling && c.JudgeTimeout(&task.TaskAssignedAt))
}

func (c *Coordinator) JudgeTimeout(t *time.Time) bool {
	return time.Since(*t) > TimeoutLimit
}

func (c *Coordinator) AssignMapTask(taskID int, pid int, task *Task, reply *Reply) {
	reply.TaskInput = task.TaskInput
	reply.TaskID = taskID
	reply.TaskType = MapTask
	reply.TaskNReduce = c.NReduce
	reply.MapTaskCount = len(c.MapTasks)
	c.MapTasks[taskID].Pid = pid
	c.MapTasks[taskID].TaskStatus = Handling
	c.MapTasks[taskID].TaskAssignedAt = time.Now()
}

func (c *Coordinator) AssignReduceTask(taskID int, pid int, task *Task, reply *Reply) {
	reply.TaskID = taskID
	reply.TaskType = ReduceTask
	reply.TaskNReduce = c.NReduce
	reply.MapTaskCount = len(c.MapTasks)
	c.ReduceTasks[taskID].Pid = pid
	c.ReduceTasks[taskID].TaskStatus = Handling
	c.ReduceTasks[taskID].TaskAssignedAt = time.Now()
}

func (c *Coordinator) RequestTask(args *Args, reply *Reply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.Stage == StageMap {
		DoneCount := 0
		for i, task := range c.MapTasks {
			if c.Assignable(&task) {
				// TaskID就是文件序号
				c.AssignMapTask(i, args.Pid, &task, reply)
				return nil
			} else if task.TaskStatus == Done {
				DoneCount++
			}
		}

		if DoneCount == len(c.MapTasks) {
			c.Stage = StageReduce
		} else {
			reply.TaskType = SleepTask
			return nil
		}
	}

	if c.Stage == StageReduce {
		DoneCount := 0
		for i, task := range c.ReduceTasks {
			if c.Assignable(&task) {
				// TaskID就是分片序号
				c.AssignReduceTask(i, args.Pid, &task, reply)
				return nil
			} else if task.TaskStatus == Done {
				DoneCount++
			}
		}

		if DoneCount == len(c.ReduceTasks) {
			c.Stage = StageEnd
		} else {
			reply.TaskType = SleepTask
			return nil
		}
	}

	reply.TaskType = QuitTask
	return nil
}

func (c *Coordinator) ReportTask(args *Args, reply *Reply) error {
	//c.Lock()
	//defer c.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskStatus == Done && args.Pid == c.MapTasks[args.TaskID].Pid {
			c.MapTasks[args.TaskID].TaskStatus = Done
		}
	case ReduceTask:
		if args.TaskStatus == Done && args.Pid == c.ReduceTasks[args.TaskID].Pid {
			c.ReduceTasks[args.TaskID].TaskStatus = Done
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c = Coordinator{
		NReduce:     nReduce,
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
		Stage:       StageMap,
		Mutex:       sync.Mutex{},
	}

	c.init(files)

	c.server()
	return &c
}
