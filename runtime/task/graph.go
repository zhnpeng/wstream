package task

import (
	"github.com/wandouz/wstream/utils/graph"
)

type taskNode struct {
	id   int
	task *Task
}

func newTaskNode(id int, task *Task) *taskNode {
	return &taskNode{
		id:   id,
		task: task,
	}
}

type TaskGraph struct {
	vertices map[int]*taskNode
	graph    *graph.Mutable
}

func NewTaskGraph() *TaskGraph {
	return &TaskGraph{
		vertices: make(map[int]*taskNode),
		graph:    graph.New(0),
	}
}

func (g *TaskGraph) AddTask(task *Task) {
	id := g.graph.AddVertex()
	node := newTaskNode(id, task)
	task.SetTaskNode(node)
	g.vertices[id] = node
}

func (g *TaskGraph) AddTaskEdge(from, to *Task) error {
	if !g.existsTask(from) {
		g.AddTask(from)
	}
	if !g.existsTask(to) {
		g.AddTask(to)
	}
	fromID := from.GetTaskNode().id
	toID := to.GetTaskNode().id
	return g.graph.AddEdge(fromID, toID)
}

func (g *TaskGraph) existsTask(task *Task) bool {
	node := task.GetTaskNode()
	if node == nil {
		return false
	}
	if _, ok := g.vertices[node.id]; ok {
		return true
	}
	return false
}
