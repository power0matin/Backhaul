package network

import "testing"

func TestResolveRemoteAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantPort int
		wantAddr string
		wantErr  bool
	}{
		{name: "port only", input: "443", wantPort: 443, wantAddr: "127.0.0.1:443"},
		{name: "ipv4", input: "192.0.2.10:8443", wantPort: 8443, wantAddr: "192.0.2.10:8443"},
		{name: "hostname", input: "example.test:53", wantPort: 53, wantAddr: "example.test:53"},
		{name: "ipv6", input: "[2001:db8::1]:443", wantPort: 443, wantAddr: "[2001:db8::1]:443"},
		{name: "zero port", input: "example.test:0", wantErr: true},
		{name: "too large", input: "example.test:65536", wantErr: true},
		{name: "missing port", input: "example.test:", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			port, addr, err := ResolveRemoteAddr(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ResolveRemoteAddr(%q) unexpectedly succeeded: port=%d addr=%q", tt.input, port, addr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveRemoteAddr(%q): %v", tt.input, err)
			}
			if port != tt.wantPort || addr != tt.wantAddr {
				t.Fatalf("ResolveRemoteAddr(%q) = (%d, %q), want (%d, %q)", tt.input, port, addr, tt.wantPort, tt.wantAddr)
			}
		})
	}
}

func FuzzResolveRemoteAddr(f *testing.F) {
	for _, seed := range []string{"443", "localhost:443", "[2001:db8::1]:53", "", "::1:443", "99999"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		port, _, err := ResolveRemoteAddr(value)
		if err == nil && (port < 1 || port > 65535) {
			t.Fatalf("successful resolution returned out-of-range port %d", port)
		}
	})
}
