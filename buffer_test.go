package mps_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/blorticus-go/mps"
)

var _ = Describe("FixedSlidingBuffer", func() {
	It("should panic if size is 0", func() {
		Expect(func() { mps.NewFixedSlidingBuffer(0) }).To(Panic())
	})

	Describe("FixedSlidingBuffer", func() {
		var buffer *mps.FixedSlidingBuffer

		BeforeEach(func() {
			buffer = mps.NewFixedSlidingBuffer(10)
		})

		It("should create a new FixedSlidingBuffer with the specified size", func() {
			Expect(buffer).NotTo(BeNil())
			Expect(buffer.Length()).To(Equal(0))
			Expect(buffer.Unused()).To(Equal(10))
			Expect(len(buffer.Peek())).To(Equal(0))
		})

		When("data a single Write is performed where the length of written data is less than the buffer size", func() {
			It("should accept the write and return expected amounts data", func() {
				By("writing data to the buffer")
				buffer.Write([]byte("1234"))
				Expect(buffer.Length()).To(Equal(4))
				Expect(buffer.Unused()).To(Equal(6))
				Expect(buffer.Peek()).To(Equal([]byte("1234")))

				By("performing a Read")
				readBuffer := make([]byte, 10)
				n, err := buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))
				Expect(readBuffer[:n]).To(Equal([]byte("1234")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))

				By("performing a second Read without having written additional data")
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(0))
			})
		})

		When("data are written twice without exceeding the buffer size", func() {
			It("should accept the writes and return expected amounts of data", func() {
				By("writing data set one to the buffer")
				n, err := buffer.Write([]byte("1234"))
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))
				Expect(buffer.Length()).To(Equal(4))
				Expect(buffer.Unused()).To(Equal(6))
				Expect(buffer.Peek()).To(Equal([]byte("1234")))

				By("writing data set two to the buffer")
				n, err = buffer.Write([]byte("5678"))
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))
				Expect(buffer.Length()).To(Equal(8))
				Expect(buffer.Unused()).To(Equal(2))
				Expect(buffer.Peek()).To(Equal([]byte("12345678")))

				By("performing a Read")
				readBuffer := make([]byte, 10)
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(8))
				Expect(readBuffer[:n]).To(Equal([]byte("12345678")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))

				By("performing a second Read without having written additional data")
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(0))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))
			})
		})

		When("data are written that exceeds the buffer size", func() {
			It("should write up to the length of the buffer and return the BufferLengthExceededError", func() {
				By("writing data to the buffer")
				n, err := buffer.Write([]byte("12345678901234567890"))
				Expect(err).To(Equal(mps.ErrBufferLengthExceeded))
				Expect(n).To(Equal(10))
				Expect(buffer.Length()).To(Equal(10))
				Expect(buffer.Unused()).To(Equal(0))
				Expect(buffer.Peek()).To(Equal([]byte("1234567890")))

				By("performing a Read")
				readBuffer := make([]byte, 10)
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(10))
				Expect(readBuffer[:n]).To(Equal([]byte("1234567890")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))
			})
		})

		When("data are written, then read, then second write is performed that does not exceed the buffer size", func() {
			It("should accept the writes without error and return expected amounts of data", func() {
				By("writing data set one to the buffer")
				n, err := buffer.Write([]byte("1234"))
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))

				By("performing a Read")
				readBuffer := make([]byte, 10)
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))
				Expect(readBuffer[:n]).To(Equal([]byte("1234")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))

				By("writing data set two to the buffer")
				n, err = buffer.Write([]byte("567890123"))
				Expect(err).To(BeNil())
				Expect(n).To(Equal(9))
				Expect(buffer.Length()).To(Equal(9))
				Expect(buffer.Unused()).To(Equal(1))
				Expect(buffer.Peek()).To(Equal([]byte("567890123")))

				By("performing a second Read")
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(9))
				Expect(readBuffer[:n]).To(Equal([]byte("567890123")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))
			})
		})

		When("data are written, then read, then second write is performed that does exceed the buffer size", func() {
			It("should write up to the length of the buffer and return the BufferLengthExceededError", func() {
				By("writing data set one to the buffer")
				n, err := buffer.Write([]byte("1234"))
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))

				By("performing a Read")
				readBuffer := make([]byte, 10)
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(4))
				Expect(readBuffer[:n]).To(Equal([]byte("1234")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))

				By("writing data set two to the buffer")
				n, err = buffer.Write([]byte("56789012345"))
				Expect(err).To(Equal(mps.ErrBufferLengthExceeded))
				Expect(n).To(Equal(10))
				Expect(buffer.Length()).To(Equal(10))
				Expect(buffer.Unused()).To(Equal(0))
				Expect(buffer.Peek()).To(Equal([]byte("5678901234")))

				By("performing a second Read")
				n, err = buffer.Read(readBuffer)
				Expect(err).To(BeNil())
				Expect(n).To(Equal(10))
				Expect(readBuffer[:n]).To(Equal([]byte("5678901234")))
				Expect(buffer.Length()).To(Equal(0))
				Expect(buffer.Unused()).To(Equal(10))
				Expect(buffer.Peek()).To(Equal([]byte{}))
			})
		})
	})
})
