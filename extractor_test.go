package mps_test

import (
	"fmt"

	"github.com/blorticus-go/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/blorticus-go/mps"
)

var _ = Describe("Extractors", func() {
	Describe("StreamExtractors", func() {
		var lengthExtractor func([]byte) (uint64, error)
		var reader *mocks.Reader
		var streamExtractor *mps.StreamExtractor
		var eventChannel chan *mps.ExtractionEvent

		BeforeEach(func() {
			lengthExtractor = func(accumulatedData []byte) (lengthOfNextMessage uint64, err error) {
				if len(accumulatedData) < 5 {
					return 0, nil
				}
				return uint64(accumulatedData[4]), nil
			}

			reader = mocks.NewReader()
			streamExtractor = mps.NewStreamExtractor(lengthExtractor)
			eventChannel = make(chan *mps.ExtractionEvent)
		})

		When("the reader returns EOF immediately", func() {
			BeforeEach(func() {
				reader.AddEOF()
				go streamExtractor.StartExtracting(reader, eventChannel)
			})

			It("should emit a StreamClosed event", func() {
				reader.AddEOF()
				go streamExtractor.StartExtracting(reader, eventChannel)

				event := <-eventChannel
				Expect(event.Type).To(Equal(mps.StreamClosed))
			})
		})

		When("the reader returns an inital error", func() {
			BeforeEach(func() {
				reader.AddError(fmt.Errorf("Test error"))
				go streamExtractor.StartExtracting(reader, eventChannel)
			})

			It("should emit an ExtractionError event", func() {
				event := <-eventChannel
				Expect(event.Type).To(Equal(mps.ExtractionError))
			})
		})

		When("the reader returns a single valid message of length 10 in one read", func() {
			BeforeEach(func() {
				reader.AddGoodRead([]byte{0, 1, 2, 3, 10, 4, 5, 6, 7, 8}).AddEOF()
				go streamExtractor.StartExtracting(reader, eventChannel)
			})

			It("should emit the correct events", func() {
				By("waiting for the first event")
				event := <-eventChannel
				Expect(event.Type).To(Equal(mps.MessageReceived))
				Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 10, 4, 5, 6, 7, 8}))

				By("waiting for the second event")
				event = <-eventChannel
				Expect(event.Type).To(Equal(mps.StreamClosed))
			})
		})

		When("the reader returns a single valid message of length 20 in two reads", func() {
			When("the first read contains the length", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1, 2, 3, 10, 4}).
						AddGoodRead([]byte{5, 6, 7, 8}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 10, 4, 5, 6, 7, 8}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})

			When("the second read contains the length", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1, 2}).
						AddGoodRead([]byte{3, 10, 4, 5, 6, 7, 8}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 10, 4, 5, 6, 7, 8}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})
		})

		When("the reader returns two valid messages", func() {
			Context("in a single read", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1, 2, 3, 5, 10, 20, 30, 50, 7, 60, 70}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 5}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{10, 20, 30, 50, 7, 60, 70}))

					By("waiting for the third event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})

			Context("in two reads where each read is exactly one message", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1, 2, 3, 5}).
						AddGoodRead([]byte{10, 20, 30, 50, 7, 60, 70}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 5}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{10, 20, 30, 50, 7, 60, 70}))

					By("waiting for the third event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})

			Context("in two reads where the first read is a partial message", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1}).
						AddGoodRead([]byte{2, 3, 5, 10, 20, 30, 50, 7, 60, 70}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 5}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{10, 20, 30, 50, 7, 60, 70}))

					By("waiting for the third event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})

			Context("in four reads", func() {
				BeforeEach(func() {
					reader.
						AddGoodRead([]byte{0, 1}).
						AddGoodRead([]byte{2, 3}).
						AddGoodRead([]byte{5, 10, 20}).
						AddGoodRead([]byte{30, 50, 7, 60, 70}).
						AddEOF()
					go streamExtractor.StartExtracting(reader, eventChannel)
				})

				It("should emit the correct events", func() {
					By("waiting for the first event")
					event := <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{0, 1, 2, 3, 5}))

					By("waiting for the second event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.MessageReceived))
					Expect(event.Message).To(Equal([]byte{10, 20, 30, 50, 7, 60, 70}))

					By("waiting for the third event")
					event = <-eventChannel
					Expect(event.Type).To(Equal(mps.StreamClosed))
				})
			})
		})
	})
})
