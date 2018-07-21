package manager

import (
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	zmq "github.com/pebbe/zmq4"
)

func TestNewTaskManager(t *testing.T) {
	workers := map[string]*Worker{
		"w1": &Worker{ID: "w1"},
		"w2": &Worker{ID: "w2"},
		"w3": &Worker{ID: "w3"},
	}
	frontendData := "ipc:///tmp/frontendData.ipc"
	frontendEvent := "ipc:///tmp/frontendEvent.ipc"
	backend := "ipc:///tmp/backend.ipc"
	mgr := NewTaskManager(frontendData, frontendEvent, backend, workers)
	client, _ := zmq.NewSocket(zmq.REQ)
	defer client.Close()
	client.Connect(frontendData)
	clientEvent, _ := zmq.NewSocket(zmq.REQ)
	defer clientEvent.Close()
	clientEvent.Connect(frontendEvent)
	got := make([]string, 0)
	go func(m *TaskManager, cd, ce *zmq.Socket) {
		if m.IsReady() {
			for _, worker := range m.Workers {
				cd.SendMessage(worker.Address, "", "HELLO")
				reply, _ := cd.RecvMessage(0)
				if len(reply) == 0 {
					break
				}
				// t.Log("Client:", strings.Join(reply, "\n\t"))
				got = append(got, strings.Join(reply, ""))
			}
			//Terminate task manager
			clientEvent.SendMessage(Terminate)
		}
	}(mgr, client, clientEvent)
	mgr.Run()
	want := []string{"w1: OK", "w2: OK", "w3: OK"}
	sort.Strings(got)
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("Want: %v, got: %v", want, got)
	}
}
