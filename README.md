# golang Message Proxy System

Framework for working with message-oriented protocols.

## Overview

```go
type MessageExtractor interface {
    ExtractPendingMessages() ([][]byte, error)
}

type StreamExtractor struct {}

func NewStreamExtractor(streamReader io.Reader, lengthExtractor func(accumulatedData []byte) (length u64, err error)) *StreamExtractor {}
func (e *StreamExtractor) ExtractPendingMessages() ([][]byte, error)

type DatagramExtractor struct {}
func NewDatagramExtractor(datagramReader io.Reader) *DatagramExtractor {}
func (e *DatagramExtractor) ExtractPendingMessages() ([][]byte, error)
```