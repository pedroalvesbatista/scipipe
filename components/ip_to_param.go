package components

import (
	"strings"

	"github.com/scipipe/scipipe"
)

// IpToParamConverter takes a file target on its FilePath in-port, reads its
// content (assuming a single value), removing any newlines, spaces or tabs,
// and sends the value on the OutParam parameter port.
type IpToParamConverter struct {
	name     string
	InFile   *scipipe.InPort
	OutParam *scipipe.ParamPort
}

// Instantiate a new IpToParamConverter
func NewIpToParamConverter(wf *scipipe.Workflow, name string) *IpToParamConverter {
	p := &IpToParamConverter{
		name:     name,
		InFile:   scipipe.NewInPort(),
		OutParam: scipipe.NewParamPort(),
	}
	wf.AddProc(p)
	return p
}

func (p *IpToParamConverter) Name() string {
	return p.name
}

func (p *IpToParamConverter) InPorts() []*scipipe.InPort {
	return []*scipipe.InPort{p.InFile}
}

func (p *IpToParamConverter) OutPorts() []*scipipe.OutPort {
	return []*scipipe.OutPort{} // No normal out-ports
}

func (p *IpToParamConverter) IsConnected() bool {
	return p.InFile.IsConnected() && p.OutParam.IsConnected()
}

// Run the IpToParamConverter
func (p *IpToParamConverter) Run() {
	defer p.OutParam.Close()
	go p.InFile.RunMergeInputs()

	for ip := range p.InFile.MergedInChan {
		s := string(ip.Read())
		s = strings.Trim(s, " \r\n\t")
		p.OutParam.Send(s)
	}
}
