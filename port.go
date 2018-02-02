package scipipe

func ConnectTo(outPort *OutPort, inPort *InPort) {
	outPort.Connect(inPort)
}

func ConnectFrom(inPort *InPort, outPort *OutPort) {
	outPort.Connect(inPort)
}

// Port is a struct that contains channels, together with some other meta data
// for keeping track of connection information between processes.
type InPort struct {
	MergedInChan chan *IP
	inChans      []chan *IP
	connected    bool
	Process      WorkflowProcess
	RemotePort   *OutPort
}

func NewInPort() *InPort {
	inp := &InPort{
		MergedInChan: make(chan *IP, BUFSIZE), // This one will contain merged inputs from inChans
		inChans:      []chan *IP{},
		connected:    false,
	}
	return inp
}

func (localPort *InPort) Connect(remotePort *OutPort) {
	inBoundChan := make(chan *IP, BUFSIZE)
	localPort.AddInChan(inBoundChan)
	remotePort.AddOutChan(inBoundChan)

	localPort.RemotePort = remotePort
	remotePort.RemotePort = localPort

	localPort.SetConnectedStatus(true)
	remotePort.SetConnectedStatus(true)
}

// TODO: We need a disconnect method here as well

func (pt *InPort) AddInChan(inChan chan *IP) {
	pt.inChans = append(pt.inChans, inChan)
}

func (pt *InPort) SetConnectedStatus(connected bool) {
	pt.connected = connected
}

func (pt *InPort) IsConnected() bool {
	return pt.connected
}

// RunMerge merges (multiple) inputs on pt.inChans into pt.MergedInChan. This has to
// start running when the owning process runs, in order to merge in-ports
func (pt *InPort) RunMergeInputs() {
	defer close(pt.MergedInChan)
	for len(pt.inChans) > 0 {
		for i, ich := range pt.inChans {
			ip, ok := <-ich
			if !ok {
				// Delete in-channel at position i
				pt.inChans = append(pt.inChans[:i], pt.inChans[i+1:]...)
				break
			}
			pt.MergedInChan <- ip
		}
	}
}

func (pt *InPort) Recv() *IP {
	return <-pt.MergedInChan
}

// OutPort represents an output connection point on Processes
type OutPort struct {
	outChans   []chan *IP
	connected  bool
	Process    WorkflowProcess
	RemotePort *InPort
}

func NewOutPort() *OutPort {
	outp := &OutPort{
		outChans:  []chan *IP{},
		connected: false,
	}
	return outp
}

func (localPort *OutPort) Connect(remotePort *InPort) {
	outBoundChan := make(chan *IP, BUFSIZE)
	localPort.AddOutChan(outBoundChan)
	remotePort.AddInChan(outBoundChan)

	localPort.RemotePort = remotePort
	remotePort.RemotePort = localPort

	localPort.SetConnectedStatus(true)
	remotePort.SetConnectedStatus(true)
}

func (port *OutPort) Disconnect() {
	// TODO: Have to disconnect our channel from the remote port as well
	port.outChans = []chan *IP{}
	port.RemotePort = nil
	port.SetConnectedStatus(false)
}

func (pt *OutPort) AddOutChan(outChan chan *IP) {
	pt.outChans = append(pt.outChans, outChan)
}

func (pt *OutPort) SetConnectedStatus(connected bool) {
	pt.connected = connected
}

func (pt *OutPort) IsConnected() bool {
	return pt.connected
}

func (pt *OutPort) Send(ip *IP) {
	for i, outChan := range pt.outChans {
		Debug.Printf("Sending on outchan %d in port\n", i)
		outChan <- ip
	}
}

func (pt *OutPort) Close() {
	for i, outChan := range pt.outChans {
		Debug.Printf("Closing outchan %d in port\n", i)
		close(outChan)
	}
}

// ParamInPort is an in-port for parameter values, of string type
type ParamInPort struct {
	inChans      []chan string
	MergedInChan chan string
	connected    bool
	RemotePort   *ParamOutPort
	Process      *Process
}

// NewParamInPort returns a new parameter in-port
func NewParamInPort() *ParamInPort {
	return &ParamInPort{
		inChans:      []chan string{},
		MergedInChan: make(chan string),
	}
}

func (pp *ParamInPort) AddInChan(c chan string) {
	pp.inChans = append(pp.inChans, c)
}

func (pp *ParamInPort) Connect(rpp *ParamOutPort) {
	inBoundChan := make(chan string, BUFSIZE)

	pp.AddInChan(inBoundChan)
	rpp.AddOutChan(inBoundChan)

	pp.RemotePort = rpp
	rpp.RemotePort = pp

	pp.SetConnectedStatus(true)
	rpp.SetConnectedStatus(true)
}

func (pp *ParamInPort) Disconnect() {
	pp.RemotePort.Chan = nil
	pp.RemotePort.SetConnectedStatus(false)

	pp.RemotePort = nil
	pp.Chan = nil
	pp.SetConnectedStatus(false)
}

func (pp *ParamInPort) ConnectStr(strings ...string) {
	pp.Chan = make(chan string, BUFSIZE)
	pp.SetConnectedStatus(true)
	go func() {
		defer pp.Close()
		for _, str := range strings {
			pp.Chan <- str
		}
	}()
}

func (pp *ParamInPort) SetConnectedStatus(connected bool) {
	pp.connected = connected
}

func (pp *ParamInPort) IsConnected() bool {
	return pp.connected
}

func (pp *ParamInPort) Recv() string {
	return <-pp.Chan
}

func (pp *ParamInPort) Close() {
	close(pp.Chan)
}

// ParamOutPort is a parameter out-port, for sending parameters of string type
type ParamOutPort struct {
	outChans   []chan string
	connected  bool
	RemotePort *ParamInPort
	Process    *Process
}

func NewParamOutPort() *ParamOutPort {
	return &ParamOutPort{
		outChans: []chan string{},
	}
}

func (pp *ParamOutPort) AddOutChan(c chan string) {
	pp.outChans = append(pp.outChans, c)
}

func (pp *ParamOutPort) Connect(rpp *ParamInPort) {
	outBoundChan := make(chan string, BUFSIZE)
	pp.AddOutChan(outBoundChan)
	rpp.AddInChan(outBoundChan)

	pp.RemotePort = rpp
	rpp.RemotePort = pp

	pp.SetConnectedStatus(true)
	rpp.SetConnectedStatus(true)
}

func (pp *ParamOutPort) Disconnect() {
	pp.RemotePort.Chan = nil
	pp.RemotePort.SetConnectedStatus(false)

	pp.RemotePort = nil
	pp.Chan = nil
	pp.SetConnectedStatus(false)
}

func (pp *ParamOutPort) ConnectStr(strings ...string) {
	pp.Chan = make(chan string, BUFSIZE)
	pp.SetConnectedStatus(true)
	go func() {
		defer pp.Close()
		for _, str := range strings {
			pp.Chan <- str
		}
	}()
}

func (pp *ParamOutPort) SetConnectedStatus(connected bool) {
	pp.connected = connected
}

func (pp *ParamOutPort) IsConnected() bool {
	return pp.connected
}

func (pp *ParamOutPort) Send(param string) {
	pp.Chan <- param
}

func (pp *ParamOutPort) Close() {
	close(pp.Chan)
}
