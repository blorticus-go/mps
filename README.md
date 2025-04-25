# golang Message Proxy System

Framework for working with message-oriented protocols.

## Overview

```go
import (
    "net"
    "github.com/blorticus-go/mps"
    "binary/encoding"
    "fmt"
)

func radiusMessageLengthExtractor(accumulatedData []byte) (lengthOfNextMessage uint64, err error) {
    if len(accumulatedData < 4) {
        return 0, nil
    }

    len := binary.BigEndian.Uint16(accumulatedData[2:4])
    if len > 4096 {
        return 0, mps.MaximumAllowedLengthExceededError
    }

    if len < 20 {
        return 0, fmt.Errorf("minimum length must be >= 20, length value is (%d)", len)
    }

    return len, nil
}

func respondToMessage(encodedMessage []byte, conn net.Conn) {
    // ...
}

func main() {
    c, err := net.Dial("tcp", "localhost:1812")
    if err != nil { panic(err) }

    extractor := mps.NewStreamExtractor()
    eventChannel := make(chan *mps.ExtractionEvent)

    go extractor.StartExtracting(eventChannel)

    for {
        event := <-eventChannel

        switch event.Type {
            case mps.MessageReceived:
                respondToMessage(event.Message, c)

            case mps.ExtractionError:
                panic("An error occured while extracting: ", mps.Error)

            case mps.StreamClosed:
                fmt.Println("stream closed")
                return
        }
    }
}
```