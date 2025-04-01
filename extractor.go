package mps

import "io"

type ExtractEventType int

const (
	MessageReceived ExtractEventType = iota
	ExtractionError
	StreamClosed
)

type ExtractionEvent struct {
	Type    ExtractEventType
	Message []byte
	Reader  io.Reader
	Error   error
}

type Extractor interface {
	StartExtracting(reader io.Reader, eventChannel chan<- *ExtractionEvent)
}

type MaximumAllowedLengthExceededError struct{}

func (e *MaximumAllowedLengthExceededError) Error() string {
	return "Maximum allowed length exceeded"
}

type LengthExtractorMethod func(accumulatedData []byte) (lengthOfNextMessage uint64, err error)

type StreamExtractor struct {
	lengthExtractor LengthExtractorMethod
	bufferSizeHint  int
	pendingData     []byte
}

func NewStreamExtractor(lengthExtractor LengthExtractorMethod) *StreamExtractor {
	return &StreamExtractor{
		lengthExtractor: lengthExtractor,
		bufferSizeHint:  9126,
		pendingData:     nil,
	}
}

func (e *StreamExtractor) SetBufferSizeHint(size int) {
	if size > 0 {
		e.bufferSizeHint = size
	}
}

func (e *StreamExtractor) StartExtracting(reader io.Reader, eventChannel chan<- *ExtractionEvent) {
	e.pendingData = make([]byte, 0, e.bufferSizeHint)

	for {
		readBuffer := make([]byte, e.bufferSizeHint)

		bytesRead, err := reader.Read(readBuffer)
		e.pendingData = append(e.pendingData, readBuffer[:bytesRead]...)

		if err != nil && err != io.EOF {
			eventChannel <- &ExtractionEvent{
				Type:   ExtractionError,
				Reader: reader,
				Error:  err,
			}
			return
		}

		for {
			nextMessageLength, err := e.lengthExtractor(e.pendingData)
			if err != nil {
				eventChannel <- &ExtractionEvent{
					Type:   ExtractionError,
					Reader: reader,
					Error:  err,
				}
				return
			}

			if nextMessageLength > 0 && nextMessageLength <= uint64(len(e.pendingData)) {
				message := make([]byte, nextMessageLength)
				copy(message, e.pendingData[:nextMessageLength])
				eventChannel <- &ExtractionEvent{
					Type:    MessageReceived,
					Message: message,
					Reader:  reader,
				}
				e.pendingData = e.pendingData[nextMessageLength:]
			} else {
				break
			}
		}

		if err == io.EOF {
			eventChannel <- &ExtractionEvent{
				Type:   StreamClosed,
				Reader: reader,
				Error:  err,
			}

			return
		}
	}
}

type DatagramExtractor struct {
	bufferSizeHint int
}

func NewDatagramExtractor() *DatagramExtractor {
	return &DatagramExtractor{
		bufferSizeHint: 65535,
	}
}

func (e *DatagramExtractor) SetBufferSizeHint(size int) {
	if size > 0 {
		e.bufferSizeHint = size
	}
}

func (e *DatagramExtractor) StartExtracting(reader io.Reader, eventChannel chan<- *ExtractionEvent) {
	for {
		readBuffer := make([]byte, e.bufferSizeHint)

		bytesRead, err := reader.Read(readBuffer)
		if err != nil && err != io.EOF {
			eventChannel <- &ExtractionEvent{
				Type:   ExtractionError,
				Reader: reader,
				Error:  err,
			}
			return
		}

		eventChannel <- &ExtractionEvent{
			Type:    MessageReceived,
			Message: readBuffer[:bytesRead],
			Reader:  reader,
		}

		if err == io.EOF {
			eventChannel <- &ExtractionEvent{
				Type:   StreamClosed,
				Reader: reader,
				Error:  err,
			}
			return
		}
	}
}
