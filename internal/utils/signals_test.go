package utils

import "testing"

func TestControlSignalValuesRemainWireCompatible(t *testing.T) {
	want := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	got := []byte{SG_HB, SG_Chan, SG_Ping, SG_Closed, SG_TCP, SG_UDP, SG_RTT, SG_MuxRetire}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("control signal %d = %d, want %d", i, got[i], want[i])
		}
	}
}
