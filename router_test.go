package mps_test

import (
	"context"
	"fmt"
	"net"
	"syscall"

	"github.com/blorticus-go/mps"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Router Tests", func() {
	Describe("Router with a single Listener", func() {
		var router *mps.MessageRouter
		var routerEventChannel chan *mps.MessageRouterEvent
		var flowListener *mps.FlowListener

		BeforeEach(func() {
			lconfig := net.ListenConfig{
				Control: func(network, address string, c syscall.RawConn) error {
					return c.Control(func(fd uintptr) {
						if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
							panic(fmt.Sprintf("failed to Setsockopt SO_REUSEADDR: %s", err))
						}
					})
				},
			}

			listener, err := lconfig.Listen(context.Background(), "tcp", "localhost:30000")
			if err != nil {
				panic(fmt.Sprintf("failed to start listener: %s", err))
			}

			routerEventChannel = make(chan *mps.MessageRouterEvent)
			extractor := mps.NewStreamExtractor(func(accumulatedData []byte) (lengthOfNextMessage uint64, err error) {
				if len(accumulatedData) >= 10 {
					return 10, nil
				}

				return 0, nil
			}, 10)

			router = mps.NewMessageRouter()
			go router.RouteMessages(routerEventChannel)

			flowListener = mps.NewFlowListenerFromNetListener(listener, extractor)
			router.AddListener(flowListener)
		})

		When("A client connects to the router listener, sends 3 messages, then closes", func() {
			var clientConn net.Conn
			BeforeEach(func() {
				var err error
				clientConn, err = net.Dial("tcp", "localhost:30000")
				if err != nil {
					panic(fmt.Sprintf("failed to dial: %s", err))
				}
			})

			AfterEach(func() {
				clientConn.Close()
			})

			It("should send the correct router events in the correct order", func() {
				By("waiting for the first event")
				event := <-routerEventChannel

				Expect(event.Type).To(Equal(mps.RouterIncomingFlow))
				Expect(event.Error).To(BeNil())
				Expect(event.Listener).To(Equal(flowListener))
				Expect(event.Flow.LocalAddr().String()).To(Equal("127.0.0.1:30000"))

				incomingMessageChannel := make(chan []byte)

				router.SetRoute(event.Flow, mps.NewCallbackTarget(func(m mps.Message, sourceFlow *mps.Flow) error {
					incomingMessageChannel <- m
					return nil
				}))

				By("sending first client message and checking next received message")
				if _, err := clientConn.Write([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}); err != nil {
					panic(fmt.Sprintf("on Write to clientConn: %s", err))
				}

				incoming := <-incomingMessageChannel
				Expect(incoming).To(Equal([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}))

				By("second second client message in two chunks and checking next received message")
				if _, err := clientConn.Write([]byte{10, 11, 12, 13, 14, 15}); err != nil {
					panic(fmt.Sprintf("on Write to clientConn: %s", err))
				}
				if _, err := clientConn.Write([]byte{16, 17, 18, 19}); err != nil {
					panic(fmt.Sprintf("on Write to clientConn: %s", err))
				}

				incoming = <-incomingMessageChannel
				Expect(incoming).To(Equal([]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}))

				By("sending third client message with some extra at the end and checking next received message")
				if _, err := clientConn.Write([]byte{20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}); err != nil {
					panic(fmt.Sprintf("on Write to clientConn: %s", err))
				}

				incoming = <-incomingMessageChannel
				Expect(incoming).To(Equal([]byte{20, 21, 22, 23, 24, 25, 26, 27, 28, 29}))

				By("closing the client connection and waiting for router event")
				clientConn.Close()
				event = <-routerEventChannel

				Expect(event.Type).To(Equal(mps.RouterFlowClosed))
				Expect(event.Error).To(BeNil())
				Expect(event.Listener).To(BeNil())
				Expect(event.Flow.LocalAddr().String()).To(Equal("127.0.0.1:30000"))

			})
		})
	})
})
