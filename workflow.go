// Package scipipe is a library for writing scientific workflows (sometimes
// also called "pipelines") of shell commands that depend on each other, in the
// Go programming languages. It was initially designed for problems in
// cheminformatics and bioinformatics, but should apply equally well to any
// domain involving complex pipelines of interdependent shell commands.
package scipipe

import (
	"sync"
)

// ----------------------------------------------------------------------------
// Workflow
// ----------------------------------------------------------------------------

type Workflow struct {
	name              string
	procs             map[string]WorkflowProcess
	concurrentTasks   chan struct{}
	concurrentTasksMx sync.Mutex
	sink              *Sink
	driver            WorkflowProcess
}

func NewWorkflow(name string, maxConcurrentTasks int) *Workflow {
	if !LogExists {
		InitLogInfo()
	}
	sink := NewSink(name + "_default_sink")
	return &Workflow{
		name:            name,
		procs:           map[string]WorkflowProcess{},
		concurrentTasks: make(chan struct{}, maxConcurrentTasks),
		sink:            sink,
		driver:          sink,
	}
}

// WorkflowProcess is an interface for processes, specifying the fields that the
// Workflow component requires.
type WorkflowProcess interface {
	Name() string
	InPorts() []*InPort
	OutPorts() []*OutPort
	IsConnected() bool
	Run()
}

// AddProc adds a Process to the workflow, to be run when the workflow runs.
func (wf *Workflow) AddProc(proc WorkflowProcess) {
	if wf.procs[proc.Name()] != nil {
		Error.Fatalf(wf.name+" workflow: A process with name '%s' already exists in the workflow! Use a more unique name!\n", proc.Name())
	}
	wf.procs[proc.Name()] = proc
}

// AddProcs takes one or many Processes and adds them to the workflow, to be run
// when the workflow runs.
func (wf *Workflow) AddProcs(procs ...WorkflowProcess) {
	for _, proc := range procs {
		wf.AddProc(proc)
	}
}

func (wf *Workflow) NewProc(procName string, commandPattern string) *Process {
	proc := NewProc(wf, procName, commandPattern)
	return proc
}

func (wf *Workflow) Proc(procName string) WorkflowProcess {
	return wf.procs[procName]
}

func (wf *Workflow) Procs() map[string]WorkflowProcess {
	return wf.procs
}

func (wf *Workflow) Sink() *Sink {
	return wf.sink
}

func (wf *Workflow) SetSink(sink *Sink) {
	if wf.sink.IsConnected() {
		Error.Fatalln("Trying to replace a sink which is already connected. Are you combining SetSink() with ConnectFinalOutPort()? That is not allowed!")
	}
	wf.sink = sink
	wf.driver = sink
}

func (wf *Workflow) Driver() WorkflowProcess {
	return wf.driver
}

func (wf *Workflow) SetDriver(driver WorkflowProcess) {
	wf.driver = driver
}

func (wf *Workflow) IncConcurrentTasks(slots int) {
	// We must lock so that multiple processes don't end up with partially "filled slots"
	wf.concurrentTasksMx.Lock()
	for i := 0; i < slots; i++ {
		wf.concurrentTasks <- struct{}{}
		Debug.Println("Increased concurrent tasks")
	}
	wf.concurrentTasksMx.Unlock()
}

func (wf *Workflow) DecConcurrentTasks(slots int) {
	for i := 0; i < slots; i++ {
		<-wf.concurrentTasks
		Debug.Println("Decreased concurrent tasks")
	}
}

// ConnectLast connects the last (most downstream) out-ports in the workflow to
// an implicit sink process which will be used to drive the workflow. This can
// be used instead of manually creating a sink, connecting it, and setting it
// as the driver process of the workflow.
func (wf *Workflow) ConnectLast(outPort *OutPort) {
	wf.sink.Connect(outPort)
	// Make sure the sink is also the driver
	wf.driver = wf.sink
}

func (wf *Workflow) readyToRun(procs ...WorkflowProcess) bool {
	if len(procs) == 0 {
		Error.Println(wf.name + ": The workflow is empty. Did you forget to add the processes to it?")
		return false
	}
	if wf.sink == nil {
		Error.Println(wf.name + ": sink is nil!")
		return false
	}
	for _, proc := range procs {
		if !proc.IsConnected() {
			Error.Println(wf.name + ": Not everything connected. Workflow shutting down.")
			return false
		}
	}
	return true
}

func (wf *Workflow) RunProcs(driverProc WorkflowProcess, procs ...WorkflowProcess) {
	if !wf.readyToRun(procs...) {
		Error.Fatalln("Workflow not ready to run, due to previously reported errors, so exiting.")
	}
	for _, proc := range procs {
		if proc != driverProc { // Don't start the driver process in background
			Debug.Printf(wf.name+": Starting process %s in new go-routine", proc.Name())
			go proc.Run()
		}
	}
	Debug.Printf(wf.name + ": Starting sink in main go-routine")
	driverProc.Run()
}

func (wf *Workflow) RunProcsByName(driverProcName string, procNames ...string) {
	procs := []WorkflowProcess{}
	for _, procName := range procNames {
		procs = append(procs, wf.Proc(procName))
	}
	driverProc := wf.Proc(driverProcName)
	wf.RunProcs(driverProc, procs...)
}

func (wf *Workflow) Run() {
	procs := []WorkflowProcess{}
	for _, p := range wf.procs {
		procs = append(procs, p)
	}
	wf.RunProcs(wf.driver, procs...)
}
