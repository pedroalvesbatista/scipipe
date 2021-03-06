package scipipe

import (
	"fmt"
	"os/exec"
	"sync"

	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var initTestLogsLock sync.Mutex

func initTestLogs() {
	if Warning == nil {
		//InitLogDebug()
		InitLogWarning()
	}
}

func TestBasicRun(t *testing.T) {
	initTestLogs()
	wf := NewWorkflow("TestBasicRunWf", 16)

	t1 := NewProc(wf, "t1", "echo foo > {o:foo}")
	assert.IsType(t, t1.Out("foo"), NewOutPort("foo"))
	t1.PathFormatters["foo"] = func(t *Task) string {
		return "foo.txt"
	}

	t2 := NewProc(wf, "t2", "sed 's/foo/bar/g' {i:foo} > {o:bar}")
	assert.IsType(t, t2.In("foo"), NewInPort("foo"))
	assert.IsType(t, t2.Out("bar"), NewOutPort("bar"))
	t2.PathFormatters["bar"] = func(t *Task) string {
		return t.InPath("foo") + ".bar.txt"
	}

	t2.In("foo").Connect(t1.Out("foo"))

	assert.IsType(t, t2.In("foo"), NewInPort("foo"))
	assert.IsType(t, t2.Out("bar"), NewOutPort("bar"))

	wf.Run()
	cleanFiles("foo.txt", "foo.txt.bar.txt")
}

func TestConnectBackwards(t *testing.T) {
	initTestLogs()

	wf := NewWorkflow("TestConnectBackwards", 16)

	t1 := wf.NewProc("t1", "echo foo > {o:foo}")
	t1.SetPathCustom("foo", func(t *Task) string { return "foo.txt" })

	t2 := wf.NewProc("t2", "sed 's/foo/bar/g' {i:foo} > {o:bar}")
	t2.SetPathCustom("bar", func(t *Task) string { return t.InPath("foo") + ".bar.txt" })

	t1.Out("foo").Connect(t2.In("foo"))

	wf.Run()

	cleanFiles("foo.txt", "foo.txt.bar.txt")
}

func TestParameterCommand(t *testing.T) {
	initTestLogs()
	wf := NewWorkflow("TestParameterCommandWf", 16)

	cmb := NewCombinatoricsProcess("cmb")
	wf.AddProc(cmb)

	// An abc file printer
	abc := NewProc(wf, "abc", "echo {p:a} {p:b} {p:c} > {o:out}")
	abc.SetPathCustom("out", func(task *Task) string {
		return fmt.Sprintf(
			"%s_%s_%s.txt",
			task.Param("a"),
			task.Param("b"),
			task.Param("c"),
		)
	})
	abc.ParamInPort("a").Connect(cmb.A)
	abc.ParamInPort("b").Connect(cmb.B)
	abc.ParamInPort("c").Connect(cmb.C)

	// A printer process
	prt := NewProc(wf, "prt", "cat {i:in} >> /tmp/log.txt; rm {i:in} {i:in}.audit.json")
	prt.In("in").Connect(abc.Out("out"))

	wf.Run()

	// Run tests
	_, err := os.Stat("/tmp/log.txt")
	assert.Nil(t, err)

	cleanFiles("/tmp/log.txt")
}

func TestProcessWithoutInputsOutputs(t *testing.T) {
	wf := NewWorkflow("test_wf", 16)
	initTestLogs()
	Debug.Println("Starting test TestProcessWithoutInputsOutputs")

	f := "/tmp/hej.txt"
	tsk := NewProc(wf, "tsk", "echo hej > "+f)
	tsk.Run()
	_, err := os.Stat(f)
	assert.Nil(t, err, fmt.Sprintf("File is missing: %s", f))
	cleanFiles(f)
}

func TestDontOverWriteExistingOutputs(t *testing.T) {
	initTestLogs()
	Debug.Println("Starting test TestDontOverWriteExistingOutputs")
	wf := NewWorkflow("TestDontOverWriteExistingOutputsWf1", 16)

	f := "/tmp/hej.txt"

	// Assert file does not exist before running
	_, e1 := os.Stat(f)
	assert.NotNil(t, e1)

	// Run pipeline a first time
	tsk := NewProc(wf, "tsk", "echo hej > {o:hej1}")
	tsk.SetPathStatic("hej1", f)

	prt := NewProc(wf, "prt", "echo {i:in1} Done!")
	prt.In("in1").Connect(tsk.Out("hej1"))

	wf.Run()

	// Assert file DO exist after running
	fiBef, e2 := os.Stat(f)
	assert.Nil(t, e2)

	// Get modified time before
	mtBef := fiBef.ModTime()

	// Make sure some time has passed before the second write
	time.Sleep(1 * time.Millisecond)

	Debug.Println("Try running the same workflow again ...")
	wf = NewWorkflow("TestDontOverWriteExistingOutputsWf2", 16)

	// Run again with different output
	tsk = NewProc(wf, "tsk", "echo hej > {o:hej2}")
	tsk.PathFormatters["hej2"] = func(task *Task) string { return f }

	prt = NewProc(wf, "prt", "echo {i:in2} Done!")
	prt.In("in2").Connect(tsk.Out("hej2"))

	wf.Run()

	// Assert exists
	fiAft, e3 := os.Stat(f)
	assert.Nil(t, e3)

	// Get modified time AFTER second run
	mtAft := fiAft.ModTime()

	// Assert file is not modified!
	assert.EqualValues(t, mtBef, mtAft)

	cleanFiles(f)
}

// Make sure that outputs are returned in order, even though they are
// spawned to work in parallel.
func TestSendOrderedOutputs(t *testing.T) {
	initTestLogs()

	fnames := []string{}
	for i := 1; i <= 10; i++ {
		fnames = append(fnames, fmt.Sprintf("/tmp/f%d.txt", i))
	}

	wf := NewWorkflow("test_wf", 16)
	ig := NewIPGen(wf, "ipgen", fnames...)

	fc := NewProc(wf, "fc", "echo {i:in} > {o:out}")
	fc.SetPathExtend("in", "out", "")
	fc.In("in").Connect(ig.Out)

	sl := NewProc(wf, "sl", "cat {i:in} > {o:out}")
	sl.SetPathExtend("in", "out", ".copy.txt")
	sl.In("in").Connect(fc.Out("out"))

	assert.NotNil(t, sl.Out)

	var expFname string
	i := 1

	tempPort := NewInPort("temp")
	ConnectFrom(tempPort, sl.Out("out"))

	// Should not start go-routines before connection stuff is done
	go ig.Run()
	go fc.Run()
	go sl.Run()

	for ft := range tempPort.Chan {
		Debug.Printf("TestSendOrderedOutputs: Looping over item %d ...\n", i)
		expFname = fmt.Sprintf("/tmp/f%d.txt.copy.txt", i)
		assert.EqualValues(t, expFname, ft.Path())
		Debug.Printf("TestSendOrderedOutputs: Looping over item %d Done.\n", i)
		i++
	}

	Debug.Println("TestSendOrderedOutputs: Done with loop ...")

	expFnames := []string{}
	for i := 1; i <= 10; i++ {
		expFnames = append(expFnames, fmt.Sprintf("/tmp/f%d.txt.copy.txt", i))
	}
	cleanFiles(fnames...)
	cleanFiles(expFnames...)
}

// Test that streaming works
func TestStreaming(t *testing.T) {
	initTestLogs()
	wf := NewWorkflow("TestStreamingWf", 16)

	// Init processes
	ls := NewProc(wf, "ls", "ls -l / > {os:lsl}")
	ls.PathFormatters["lsl"] = func(task *Task) string {
		return "/tmp/lsl.txt"
	}

	grp := NewProc(wf, "grp", "grep etc {i:in} > {o:grepped}")
	grp.PathFormatters["grepped"] = func(task *Task) string {
		return task.InPath("in") + ".grepped.txt"
	}

	// Connect
	grp.In("in").Connect(ls.Out("lsl"))

	// Run
	wf.Run()

	// Assert that a file exists
	_, err1 := os.Stat("/tmp/lsl.txt.fifo")
	assert.Nil(t, err1, "FIFO file does not exist, which it should!")

	// Assert otuput file exists
	_, err2 := os.Stat("/tmp/lsl.txt.grepped.txt")
	assert.Nil(t, err2, "File missing!")

	// Clean up
	cleanFiles("/tmp/lsl.txt", "/tmp/lsl.txt.grepped.txt")
	cleanFiles("/tmp/lsl.txt.fifo")
}

func TestSubStreamReduceInPlaceHolder(t *testing.T) {
	initTestLogs()

	exec.Command("bash", "-c", "echo 1 > /tmp/file1.txt").CombinedOutput()
	exec.Command("bash", "-c", "echo 2 > /tmp/file2.txt").CombinedOutput()
	exec.Command("bash", "-c", "echo 3 > /tmp/file3.txt").CombinedOutput()

	wf := NewWorkflow("TestSubStreamReduceInPlaceHolderWf", 16)

	// Create some input files

	ipg := NewIPGen(wf, "ipg", "/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt")

	sts := NewStreamToSubStream(wf, "str_to_substr")
	sts.In.Connect(ipg.Out)

	cat := wf.NewProc("concatenate", "cat {i:infiles:r: } > {o:merged}")
	cat.SetPathStatic("merged", "/tmp/substream_merged.txt")
	cat.In("infiles").Connect(sts.OutSubStream)

	wf.Run()

	_, err1 := os.Stat("/tmp/file1.txt")
	assert.Nil(t, err1, "File missing!")

	_, err2 := os.Stat("/tmp/file2.txt")
	assert.Nil(t, err2, "File missing!")

	_, err3 := os.Stat("/tmp/file3.txt")
	assert.Nil(t, err3, "File missing!")

	_, err4 := os.Stat("/tmp/substream_merged.txt")
	assert.Nil(t, err4, "File missing!")

	cleanFiles("/tmp/file1.txt", "/tmp/file2.txt", "/tmp/file3.txt", "/tmp/substream_merged.txt")
}

func TestMultipleLastProcs(t *testing.T) {
	initTestLogs()

	wf := NewWorkflow("TestMultipleLastProcs_WF", 16)
	strs := []string{"hey", "how", "hoo"}

	for _, str := range strs {
		writeStr := wf.NewProc("writestr_"+str, "echo "+str+" > {o:out}")
		writeStr.SetPathStatic("out", "/tmp/"+str+".txt")

		catStr := wf.NewProc("catstr_"+str, "cat {i:in} > {o:out}")
		catStr.SetPathExtend("in", "out", ".cat.txt")

		catStr.In("in").Connect(writeStr.Out("out"))
	}

	wf.Run()

	for _, str := range strs {
		path := "/tmp/" + str + ".txt"
		_, err := os.Stat(path)
		assert.Nil(t, err, "File missing: "+path)
	}

	for _, str := range strs {
		path := "/tmp/" + str + ".txt.cat.txt"
		_, err := os.Stat(path)
		assert.Nil(t, err, "File missing: "+path)
	}

	cleanFiles("/tmp/hey.txt", "/tmp/how.txt", "/tmp/hoo.txt")
	cleanFiles("/tmp/hey.txt.cat.txt", "/tmp/how.txt.cat.txt", "/tmp/hoo.txt.cat.txt")
}

func TestPassOnKeys(t *testing.T) {
	wf := NewWorkflow("TestPassOnKeys_WF", 4)

	hey := wf.NewProc("create_file", "echo hey > {o:heyfile}")
	hey.SetPathStatic("heyfile", "/tmp/hey.txt")

	key := NewMapToKeys(wf, "add_key", func(ip *IP) map[string]string {
		return map[string]string{"hey": "you"}
	})
	key.In.Connect(hey.Out("heyfile"))

	you := wf.NewProc("add_you", "echo '$(cat {i:infile}) you' > {o:youfile}")
	you.SetPathExtend("infile", "youfile", ".you.txt")
	you.In("infile").Connect(key.Out)

	wf.Run()

	dat, err := ioutil.ReadFile("/tmp/hey.txt.you.txt.audit.json")
	CheckErr(err)
	auditInfo := &AuditInfo{}
	err = json.Unmarshal(dat, auditInfo)
	CheckErr(err)

	assert.EqualValues(t, "you", auditInfo.Keys["hey"], "Audit info does not contain passed on keys")

	cleanFiles("/tmp/hey.txt", "/tmp/hey.txt.you.txt")
}

// --------------------------------------------------------------------------------
// Helper functions
// --------------------------------------------------------------------------------
func cleanFiles(fileNames ...string) {
	Debug.Println("Starting to remove files:", fileNames)
	for _, fileName := range fileNames {
		auditFileName := fileName + ".audit.json"
		if _, err := os.Stat(fileName); err == nil {
			os.Remove(fileName)
			Debug.Println("Successfully removed file", fileName)
		}
		if _, err := os.Stat(auditFileName); err == nil {
			os.Remove(auditFileName)
			Debug.Println("Successfully removed audit.json file", auditFileName)
		}
	}
}

// --------------------------------------------------------------------------------
// CombinatoricsProcess helper process
// --------------------------------------------------------------------------------
type CombinatoricsProcess struct {
	name string
	A    *ParamOutPort
	B    *ParamOutPort
	C    *ParamOutPort
}

func NewCombinatoricsProcess(name string) *CombinatoricsProcess {
	a := NewParamOutPort("a")
	b := NewParamOutPort("b")
	c := NewParamOutPort("c")
	p := &CombinatoricsProcess{
		A:    a,
		B:    b,
		C:    c,
		name: name,
	}
	a.Process = p
	b.Process = p
	c.Process = p
	return p
}

func (p *CombinatoricsProcess) InPorts() map[string]*InPort {
	return map[string]*InPort{}
}
func (p *CombinatoricsProcess) OutPorts() map[string]*OutPort {
	return map[string]*OutPort{}
}
func (p *CombinatoricsProcess) ParamInPorts() map[string]*ParamInPort {
	return map[string]*ParamInPort{}
}
func (p *CombinatoricsProcess) ParamOutPorts() map[string]*ParamOutPort {
	return map[string]*ParamOutPort{
		p.A.Name(): p.A,
		p.B.Name(): p.B,
		p.C.Name(): p.C,
	}
}

func (p *CombinatoricsProcess) Run() {
	defer p.A.Close()
	defer p.B.Close()
	defer p.C.Close()

	for _, a := range []string{"a1", "a2", "a3"} {
		for _, b := range []string{"b1", "b2", "b3"} {
			for _, c := range []string{"c1", "c2", "c3"} {
				p.A.Send(a)
				p.B.Send(b)
				p.C.Send(c)
			}
		}
	}
}

func (p *CombinatoricsProcess) Name() string {
	return p.name
}

func (p *CombinatoricsProcess) Connected() bool { return true }

// --------------------------------------------------------------------------------
// StreamToSubstream helper process
// --------------------------------------------------------------------------------
type StreamToSubStream struct {
	EmptyWorkflowProcess
	name         string
	In           *InPort
	OutSubStream *OutPort
}

func NewStreamToSubStream(wf *Workflow, name string) *StreamToSubStream {
	sts := &StreamToSubStream{
		In:           NewInPort("in"),
		OutSubStream: NewOutPort("out_substream"),
	}
	sts.In.Process = sts
	sts.OutSubStream.Process = sts
	wf.AddProc(sts)
	return sts
}

func (p *StreamToSubStream) InPorts() map[string]*InPort {
	return map[string]*InPort{
		p.In.Name(): p.In,
	}
}

func (p *StreamToSubStream) OutPorts() map[string]*OutPort {
	return map[string]*OutPort{
		p.OutSubStream.Name(): p.OutSubStream,
	}
}

func (p *StreamToSubStream) Run() {
	defer p.OutSubStream.Close()

	subStreamIP := NewIP("")
	subStreamIP.SubStream = p.In

	p.OutSubStream.Send(subStreamIP)
}

func (p *StreamToSubStream) Name() string {
	return "StreamToSubstream"
}

func (p *StreamToSubStream) Connected() bool {
	return p.In.Connected() && p.OutSubStream.Connected()
}

// --------------------------------------------------------------------------------
// MapToKey helper process
// --------------------------------------------------------------------------------
type MapToKeys struct {
	EmptyWorkflowProcess
	In       *InPort
	Out      *OutPort
	procName string
	mapFunc  func(ip *IP) map[string]string
}

func NewMapToKeys(wf *Workflow, name string, mapFunc func(ip *IP) map[string]string) *MapToKeys {
	mtp := &MapToKeys{
		procName: name,
		mapFunc:  mapFunc,
		In:       NewInPort("in"),
		Out:      NewOutPort("out"),
	}
	mtp.In.Process = mtp
	mtp.Out.Process = mtp
	wf.AddProc(mtp)
	return mtp
}

func (p *MapToKeys) Name() string {
	return p.procName
}

func (p *MapToKeys) InPorts() map[string]*InPort {
	return map[string]*InPort{
		p.In.Name(): p.In,
	}
}

func (p *MapToKeys) OutPorts() map[string]*OutPort {
	return map[string]*OutPort{
		p.Out.Name(): p.Out,
	}
}

func (p *MapToKeys) Connected() bool {
	return p.In.Connected() && p.Out.Connected()
}

func (p *MapToKeys) Run() {
	defer p.Out.Close()
	for ip := range p.In.Chan {
		newKeys := p.mapFunc(ip)
		ip.AddKeys(newKeys)
		ip.WriteAuditLogToFile()
		p.Out.Send(ip)
	}
}
