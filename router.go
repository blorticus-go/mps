package mps

import (
	"net"
	"sync"
)

// Object set:
//  Message -- extracted bytes that can be routed and can have attached metadata
// 	MessageRouter -- manages Flows and Listeners
//	Flow -- a socket
//  Listener -- a listening IP:proto:port with an attached Extractor
//	FlowCollection -- zero or more Flows, potentially weighted
//	Endpoint -- a target IP:proto:port
//	EndpointCollection -- zero or more Endpoints, potentially weighted
//	CollectionGroup -- zero or more Collections, potentially with priorities
//  MessageChannel -- a channel receiver for incoming Messages
//	Route -- a destination for a message, can be a Flow, an Endpoint, a Collection, a Group, or a MessageChannel

// Message is a wire-encoded messsage.
type Message []byte

// A RouteTarget is the destination to which an incoming message is delivered by a MessageRouter.
type RouteTarget interface {
	receive(m Message, sourceFlow *Flow) error
}

// MessageRouterEventType distinguishes the type of a MessageRouterEvent.
type MessageRouterEventType int

const (
	// RouterIncomingFlow indicates that a new Flow has been received through a FlowListener.  In
	// the MessageRouterEvent, this event type sets Flow and Listener.
	RouterIncomingFlow MessageRouterEventType = iota

	// RouterFlowClosed indicates that a Flow has terminated.  For connection-oriented protocols,
	// this is usually because the remote end closed the connection.  In the MessageRouterEvent,
	// this event type sets Flow.
	RouterFlowClosed

	// RouterListenerError indicates that a FlowListener experienced an error, usually on Accept.
	// When this event is raised, the identified FlowListener is closed.  In the MessageRouterEvent, this
	// event type sets Listener and Error.
	RouterListenerError

	// RouterFlowError indicates that there was an error on reading from or writing to a Flow.
	// When this event is raised, the identified Flow is closed.  In the MessageRouterEvent, this event
	// type sets Flow and Error.
	RouterFlowError
)

// MessageRouterEvents are emited by a MessageRouter while it is actively routing.
type MessageRouterEvent struct {
	Type     MessageRouterEventType
	Flow     *Flow
	Listener *FlowListener
	Error    error
}

// A MessageRouter manages Flows, represening a source of and receiver for messages.  A MessageRouter
// has a route table, mapping a source Flow (i.e., a Flow on which a message is received) and a RouteTarget.
type MessageRouter struct {
	flowListenerEventChannel   chan *flowListenerEvent
	flowExtractionEventChannel chan *flowExtractionEvent
	routeTable                 *routeTable
}

// NewMessageRouter creates a MessageRouter with an empty route table.
func NewMessageRouter() *MessageRouter {
	return &MessageRouter{
		flowListenerEventChannel:   make(chan *flowListenerEvent),
		flowExtractionEventChannel: make(chan *flowExtractionEvent),
		routeTable:                 newEmptyRouteTable(),
	}
}

// AddListener attaches a FlowListener to a router, and starts the Listener.
func (r *MessageRouter) AddListener(listener *FlowListener) *MessageRouter {
	go listener.listenForFlows(r.flowListenerEventChannel)
	return r
}

// InitiateFlowTo creates an outgoing Flow from the router to an Endpoint.
func (r *MessageRouter) InitiateFlowTo(e *Endpoint) (*Flow, error) {
	conn, err := net.Dial(e.proto, e.address)
	if err != nil {
		return nil, err
	}

	return &Flow{
		underlyingConnection: conn,
		extractor:            e.extractor,
	}, nil
}

// SetRoute sets an entry in the MessageRouter's route table.  If a route matching
// the from Flow exists, it is overwritten.
func (r *MessageRouter) SetRoute(from *Flow, to RouteTarget) *MessageRouter {
	r.routeTable.SetRoute(from, to)
	return r
}

// RouteMessages should run as a goroutine.  It performs routing according to the route
// table and emits events as messages traverse the router.
func (r *MessageRouter) RouteMessages(routerEventChannel chan<- *MessageRouterEvent) {
	for {
		select {
		case listenerEvent := <-r.flowListenerEventChannel:
			switch listenerEvent.Type {
			case flowListenerIncomingFlow:
				go listenerEvent.Flow.extractMessages(r.flowExtractionEventChannel)

				routerEventChannel <- &MessageRouterEvent{
					Type:     RouterIncomingFlow,
					Listener: listenerEvent.Source,
					Flow:     listenerEvent.Flow,
				}

			case flowListenerError:
				routerEventChannel <- &MessageRouterEvent{
					Type:     RouterListenerError,
					Listener: listenerEvent.Source,
					Error:    listenerEvent.Error,
				}
			}

		case flowExtractionEvent := <-r.flowExtractionEventChannel:
			switch flowExtractionEvent.Event.Type {
			case MessageReceived:
				flow := flowExtractionEvent.Flow

				if route := r.routeTable.GetRoute(flow); route != nil {
					route.receive(flowExtractionEvent.Event.Message, flow)
				}

			case FlowClosed:
				routerEventChannel <- &MessageRouterEvent{
					Type: RouterFlowClosed,
					Flow: flowExtractionEvent.Flow,
				}

			case ExtractionError:
				routerEventChannel <- &MessageRouterEvent{
					Type:  RouterFlowError,
					Flow:  flowExtractionEvent.Flow,
					Error: flowExtractionEvent.Event.Error,
				}
			}
		}
	}
}

type flowExtractionEvent struct {
	Event *ExtractionEvent
	Flow  *Flow
}

// A Flow represents a source of and receiver for encoded messages.  A Flow is a valid RouteTarget, in
// which case, a message from a source Flow is delivered to this Flow's remote endpoint.
type Flow struct {
	underlyingConnection net.Conn
	extractor            Extractor
}

// RemoteAddr is the net.Addr of the remote endpoint which the Flow is targeting.
func (f *Flow) RemoteAddr() net.Addr {
	return f.underlyingConnection.RemoteAddr()
}

// LocalAddr is the net.Addr of the router for this Flow.
func (f *Flow) LocalAddr() net.Addr {
	return f.underlyingConnection.LocalAddr()
}

func (f *Flow) extractMessages(eventChannel chan<- *flowExtractionEvent) {
	defer f.underlyingConnection.Close()

	extractionEventChannel := make(chan *ExtractionEvent)
	go f.extractor.StartExtracting(f.underlyingConnection, extractionEventChannel)

	for {
		extractionEvent := <-extractionEventChannel
		eventChannel <- &flowExtractionEvent{
			Event: extractionEvent,
			Flow:  f,
		}

		if extractionEvent.Type == ExtractionError {
			return
		}
	}
}

// Terminate terminates a Flow.  For connection-oriented Flows, this means the connection
// is closed.
func (f *Flow) Terminate() error {
	return f.underlyingConnection.Close()
}

// SendMessage sends an encoded message to the Flow's remote endpoint.  If an error occurs,
// the Flow is terminated.
func (f *Flow) SendMessage(encoded []byte) error {
	if _, err := f.underlyingConnection.Write(encoded); err != nil {
		f.Terminate()
		return err
	}

	return nil
}

func (f *Flow) receive(m Message, sourceFlow *Flow) error {
	return f.SendMessage(m)
}

type flowListenerEventType int

const (
	flowListenerIncomingFlow flowListenerEventType = iota
	flowListenerError
)

type flowListenerEvent struct {
	Type   flowListenerEventType
	Flow   *Flow
	Error  error
	Source *FlowListener
}

// A FlowListener listens for incoming Flows of a particular type.
type FlowListener struct {
	netListener       net.Listener
	extractorForFlows Extractor
}

// NewFlowListener creates a new FlowListener using the provided network and listener addr.
// The extractor is applied to any Flow initiated through the FlowListener.
func NewFlowListener(network, addr string, extractor Extractor) (*FlowListener, error) {
	netListener, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}

	return &FlowListener{
		netListener:       netListener,
		extractorForFlows: extractor,
	}, nil
}

// NewFlowListenerFromNetListener creates a new FlowListener using the provided netListener.
func NewFlowListenerFromNetListener(netListener net.Listener, extractor Extractor) *FlowListener {
	return &FlowListener{
		netListener:       netListener,
		extractorForFlows: extractor,
	}
}

func (l *FlowListener) listenForFlows(eventChannel chan<- *flowListenerEvent) {
	defer l.netListener.Close()

	for {
		conn, err := l.netListener.Accept()
		if err != nil {
			eventChannel <- &flowListenerEvent{
				Type:   flowListenerError,
				Error:  err,
				Source: l,
			}
		}

		eventChannel <- &flowListenerEvent{
			Type:   flowListenerIncomingFlow,
			Flow:   &Flow{underlyingConnection: conn, extractor: l.extractorForFlows},
			Source: l,
		}
	}
}

// func (l *FlowListener) terminate() {}

// An Endpoint represent a remote destination for or source of a Flow.
type Endpoint struct {
	proto     string
	address   string
	extractor Extractor
}

// NewEndpoint creates an Endpoint and uses the provided extractor for data
// arriving on a Flow associated with this Endpoint.
func NewEndpoint(proto, address string, extractor Extractor) *Endpoint {
	return &Endpoint{proto, address, extractor}
}

// A MessageHandler is a callback function used by a CallbackRouteTarget
// when a message arrives on an attached route.
type MessageHandler func(m Message, sourceFlow *Flow) error

// CallbackRouteTarget is a RouteTarget that invokes a callback every time
// a message arrives on the route.  The callback is not run as a goroutine,
// so it can block a Flow.  If the callback is doing something non-trivial,
// consider sending the message on a channel to a coroutine that is listening
// on a buffered channel, or launching a goroutine from the callback.
type CallbackRouteTarget struct {
	handler MessageHandler
}

// NewCallbackTarget creates a new CallbackTarget that uses the handler callback.
func NewCallbackTarget(handler MessageHandler) *CallbackRouteTarget {
	return &CallbackRouteTarget{
		handler,
	}
}

func (r *CallbackRouteTarget) receive(m Message, sourceFlow *Flow) error {
	return r.handler(m, sourceFlow)
}

type routeTable struct {
	m        sync.Mutex
	routeMap map[*Flow]RouteTarget
}

func newEmptyRouteTable() *routeTable {
	return &routeTable{
		routeMap: make(map[*Flow]RouteTarget),
	}
}

func (t *routeTable) SetRoute(forFlow *Flow, receiver RouteTarget) {
	t.m.Lock()
	t.routeMap[forFlow] = receiver
	t.m.Unlock()
}

func (t *routeTable) GetRoute(forFlow *Flow) RouteTarget {
	t.m.Lock()
	r := t.routeMap[forFlow]
	t.m.Unlock()

	return r
}
