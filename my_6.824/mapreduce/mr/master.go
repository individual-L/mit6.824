package mr

import(
	"time"
	"sync"
)
const TIMEOUT = 5 * time.second

//任务具有的三个状态
type TASKSTATE int
const (
	idle WORKSTATE = iota
	processing
	completed
)
//两个执行阶段
type SHAPESTATE int
const (
	MAP SHAPESTATE = iota
	REDUCE
	EXIT
)
//master储存的task信息
type masterTasks struct{
	taskState TASKSTATE
	startTime time.Time
	task *Task
}
type Task struct {
	Input string
	NReudce int
	Phase SHAPESTATE
	Output string
	Intermediate []string
	TaskId int
}
type Master struct{
	TaskQueue chan *Task
	Tasks []*masterTasks
	Phase SHAPESTATE
	NReudce int
	Intermediate [][]string //map阶段产生的R个中间文件的信息
	InputFiles    []string	
}
var mu sync.Mutex
//创建一个master
func MakeMaster(files []string,nReduce int)*Master{
	master := Master{
		TaskQueue:   make(chan(*Task,max(len(files,nReduce)))),
		Tasks:       make([]Task),
		Phase:       MAP,
		NReudce:     nReduce,
		Inputfiles:  file,
		intermediate:make([][]string,nReduce)
	}
	//用传入的files创建map任务
	master.createMapTask()
	//启动服务
	master.server()
	//定时检测是否有超时
	go master.ticktimeout()
	return &master
}
func (m Master*) createMapTask(){
	for idx, file := range m.InputFiles{
		taskk := Task{
			Input:    file,
			Phase:    m.Phase,
			NRduce:   m.NReudce,
			TaskId:   idx
		}
		TaskQueue <- &taskk
		Tasks[idx] = &masterTasks{
			taskState: idle,
			task:&taskk
		}
	}
}
func (m Master*)server(){
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
func (m Master*) ticktimeout(){
	for{
		time.Sleep(5 * time.second)
		mu.Lock()
		if(m.Phase == EXIT){
			mu.Unlock()
			return
		}
		for _,taskk range m.tasks{
			if(taskk.taskState == processing && time.Now().Sub(taskk.startTime) > TIMEOUT){
				TaskQueue <- &taskk.task
				taskk.taskState = idle
			}
		}
		mu.Unlock()
	}
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// master等待worker请求task
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	// 检测queue是否为空
	if len(m.) > 0 {
		//有就发出去
		task = <-m.TaskQueue
		reply = &task
		// 记录task的启动时间
		m.Tasks[reply.TaskId].taskState = processing
		m.Tasks[reply.TaskId].startTime = time.Now()
	} else if m.Phase == EXIT {
		reply = Task{Phase: Exit}
	} else {
		// 没有task就让worker 等待
		reply = Task{Phase: Wait}
	}
	return nil
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	defer mu.Unlock()
	if task.Phase != m.Phase || m.Tasks[task.TaskId].taskState == completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	m.Tasks[task.TaskId].taskState = completed
	go m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task)  {
	mu.Lock()
	defer mu.Unlock()
	switch task.taskState {
	case MAP:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case REDUCE:
		if m.allTaskDone() {
			//获得所以reduce task后，进入exit阶段
			m.Phase = Exit
		}
	}
}
func (m *Master) createReduceTask() {
	m.Tasks = make([]*MasterTask)
	for idx, files := range m.Intermediates {
		taskk := Task{
			taskState:     	Reduce,
			NRduce:      		m.NReduce,
			TaskId:    			idx,
			Intermediates: 	files,
		}
		m.TaskQueue <- &taskk
		m.Tasks[idx] = &MasterTask{
			taskState:    Idle,
			task: &taskk,
		}
	}
}
func (m *Master) allTaskDone() bool {
	for _, task := range m.Tasks {
		if task.taskState != completed {
			return false
		}
	}
	return true
}
