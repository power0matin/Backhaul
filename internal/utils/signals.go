package utils

const (
	SG_HB     byte = iota // for heartbeat
	SG_Chan               // for channel, req a new conn
	SG_Ping               // for ping
	SG_Closed             // for closed channel
	SG_TCP                // TCP Transport ID
	SG_UDP                // TCP Transport ID
	SG_RTT                // For RTT measurment
	// SG_MuxRetire is only sent after WSMUX peers explicitly negotiate the
	// retirement capability. Keeping it at the end preserves every existing
	// v0.7.2/v0.8.0 signal value.
	SG_MuxRetire
)

const WSMUXRetireSubprotocol = "backhaul.mux-retire.v1"
