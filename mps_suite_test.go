package mps_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMPSSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MPS Suite")
}
