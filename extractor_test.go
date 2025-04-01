package mps_test

import (
	"testing"

	"github.com/blorticus-go/mocks"
	"github.com/blorticus-go/mps"
)

func TestStreamExtractor(t *testing.T) {
	lengthExtractor := func(accumulatedData []byte) (lengthOfNextMessage uint64, err error) {
		if len(accumulatedData) < 4 {
			return 0, nil
		}
		return uint64(accumulatedData[4]), nil
	}

	r := mocks.NewReader().
		AddEOF()

	c := make(chan *mps.ExtractionEvent)
	e := mps.NewStreamExtractor(lengthExtractor)

	go e.StartExtracting(r, c)

	event := <-c
	if event.Type != mps.StreamClosed {
		t.Errorf("Expected StreamClosed event, got %v", event.Type)
	}
}
