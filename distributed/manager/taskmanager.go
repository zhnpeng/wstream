package manager

import (
	"errors"
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

/*
TaskManager response to manage task
frontendEvent used to deal with controlling event
frontendData use to deal with data
backend comunicate with workers
*/
type TaskManager struct {
	frontendEvent string // Endpoint of frontendEvent should be tcp://*:port
	frontendData  string // Endpoint of frontendData should be tcp://*:port
	backend       string // Endpoint of backend should be ipc://task_id_backend.ipc
	broker        *TaskBroker
	Workers       map[string]*Worker
}

//NewTaskManager create a new task manager
func NewTaskManager(frontendData, frontendEvent, backend string, workers map[string]*Worker) *TaskManager {
	return &TaskManager{
		frontendData:  frontendData,
		frontendEvent: frontendEvent,
		backend:       backend,
		Workers:       workers,
	}
}

//IsReady check if task manager is ready, it will block thread
func (manager *TaskManager) IsReady() bool {
	tmp := make(map[string]bool, 0)
	for id := range manager.Workers {
		tmp[id] = true
	}
	for {
		for id, worker := range manager.Workers {
			if _, ok := tmp[id]; !ok {
				continue
			}
			address, isReady := manager.Ping(id)
			if isReady == true {
				worker.IsReady = true
				worker.Address = address
				delete(tmp, id)
			}
		}
		if len(tmp) == 0 {
			break
		}
	}
	return true
}

//Ping ping worker with its id tell if it is ready
//TODO: remove this function from nanager it should be called by jobmanager
func (manager *TaskManager) Ping(workerID string) (addrOrID string, isReady bool) {
	client, _ := zmq.NewSocket(zmq.REQ)
	defer client.Close()
	client.Connect(manager.frontendEvent)
	client.SendMessage(PingSignal, "", workerID)
	reply, _ := client.RecvMessage(0)
	if len(reply) == 0 {
		return workerID, false
	}
	addrOrID, msg := unwrap(reply)
	if msg[0] == WorkerReady {
		isReady = true
	} else {
		isReady = false
	}
	return
}

//Run run task manager forever
func (manager *TaskManager) Run() {
	tbroker := &TaskBroker{
		workers: make(map[string]string),
	}
	manager.broker = tbroker
	tbroker.frontendData, _ = zmq.NewSocket(zmq.ROUTER)
	tbroker.frontendEvent, _ = zmq.NewSocket(zmq.ROUTER)
	tbroker.backend, _ = zmq.NewSocket(zmq.ROUTER)
	defer tbroker.frontendData.Close()
	defer tbroker.frontendEvent.Close()
	defer tbroker.backend.Close()
	tbroker.frontendData.Bind(manager.frontendData)
	tbroker.frontendEvent.Bind(manager.frontendEvent)
	tbroker.backend.Bind(manager.backend)

	for _, worker := range manager.Workers {
		go worker.WorkerTask(manager.backend)
	}

	//  Prepare reactor and fire it up
	tbroker.reactor = zmq.NewReactor()
	tbroker.reactor.AddSocket(tbroker.backend, zmq.POLLIN,
		func(e zmq.State) error { return HandleBackend(tbroker) })
	tbroker.reactor.AddSocket(tbroker.frontendData, zmq.POLLIN,
		func(e zmq.State) error { return HandleFrontendData(tbroker) })
	tbroker.reactor.AddSocket(tbroker.frontendEvent, zmq.POLLIN,
		func(e zmq.State) error { return HandleFrontendEvent(tbroker) })
	tbroker.reactor.Run(-1)
}

//Worker is basicaly a reader for remote subtask node
type Worker struct {
	ID      string
	Address string
	IsReady bool
}

//WorkerTask dispatch input to subtask node
//TODO: dispatch input to subtask node
func (w *Worker) WorkerTask(backend string) {
	worker, _ := zmq.NewSocket(zmq.REQ)
	defer worker.Close()
	worker.Connect(backend)

	//  Tell broker we're ready for work
	worker.SendMessage(w.ID, "", WorkerReady)

	//  Process messages as they arrive
	for {
		msg, e := worker.RecvMessage(0)
		if e != nil {
			break //  Interrupted
		}
		msg[len(msg)-1] = fmt.Sprintf("%s: OK", w.ID)
		worker.SendMessage(msg)
	}
}

//TaskBroker is load-balancer structure, passed to reactor handlers
type TaskBroker struct {
	frontendData  *zmq.Socket       //  Listen to clients
	frontendEvent *zmq.Socket       //  Listen to clients
	backend       *zmq.Socket       //  Listen to workers
	workers       map[string]string //  List of ready workers
	reactor       *zmq.Reactor
}

//  In the reactor design, each time a message arrives on a socket, the
//  reactor passes it to a handler function. We have two handlers; one
//  for the frontendData, one for the backend:

//HandleFrontendData  Handle input from client, on frontendData
func HandleFrontendData(tbroker *TaskBroker) error {
	//  Get client request, route to first available worker
	recv, err := tbroker.frontendData.RecvMessage(0)
	if err != nil {
		return err
	}
	clientAddr, data := unwrap(recv)
	workerAddr, msg := unwrap(data)
	tbroker.backend.SendMessage(workerAddr, "", clientAddr, "", msg)
	return nil
}

//HandleFrontendEvent  Handle input from client, on frontendData
func HandleFrontendEvent(tbroker *TaskBroker) error {
	//  Get client request, route to first available worker
	recv, err := tbroker.frontendEvent.RecvMessage(0)
	if err != nil {
		return err
	}
	clientAddr, data := unwrap(recv)
	signal, msg := unwrap(data)
	switch signal {
	case PingSignal:
		workerID := msg[0]
		if workerAddr, ok := tbroker.workers[workerID]; ok {
			tbroker.frontendEvent.SendMessage(clientAddr, "", workerAddr, "", WorkerReady)
			return nil
		}
		tbroker.frontendEvent.SendMessage(clientAddr, "", workerID, "", UnknownError)
		return nil
	case Terminate:
		tbroker.frontendEvent.SendMessage(clientAddr, "", OK)
		//zmq reactor termiate when receive an error
		//TODO refine this
		return errors.New("Terminate by signal")
	default:
		break
	}
	tbroker.frontendEvent.SendMessage(clientAddr, "", UnknownError)
	return nil
}

//HandleBackend  Handle input from worker, on backend
func HandleBackend(tbroker *TaskBroker) error {
	//  Use worker identity for load-balancing
	recv, err := tbroker.backend.RecvMessage(0)
	if err != nil {
		return err
	}
	addr, data := unwrap(recv)
	//data will be worker_id "" WorkerReady when is a ready signal
	//and/or be client_address "" msg otherwise
	id, msg := unwrap(data)
	if msg[0] == WorkerReady {
		tbroker.workers[id] = addr
	} else {
		tbroker.frontendData.SendMessage(data)
	}
	return nil
}
