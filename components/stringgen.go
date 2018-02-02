package components

import "github.com/scipipe/scipipe"

// StringGen takes a number of strings and returns a generator process
// which sends the strings, one by one, on its `Out` port
type StringGen struct {
	name    string
	Strings []string
	Out     *scipipe.ParamPort
}

// Instantiate a new StringGen
func NewStringGen(wf *scipipe.Workflow, name string, strings ...string) *StringGen {
	sg := &StringGen{
		name:    name,
		Out:     scipipe.NewParamPort(),
		Strings: strings,
	}
	wf.AddProc(sg)
	return sg
}

func (proc *StringGen) Name() string {
	return proc.name
}

func (p *StringGen) InPorts() []*scipipe.InPort {
	return []*scipipe.InPort{}
}

func (p *StringGen) OutPorts() []*scipipe.OutPort {
	return []*scipipe.OutPort{} // Now normal out-ports
}

// Run the StringGen
func (proc *StringGen) Run() {
	defer proc.Out.Close()
	for _, str := range proc.Strings {
		proc.Out.Send(str)
	}
}

func (proc *StringGen) IsConnected() bool {
	return proc.Out.IsConnected()
}
