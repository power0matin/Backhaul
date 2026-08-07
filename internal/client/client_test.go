package client

import "testing"

func TestEffectiveMaxPoolSizeCompatibilityDefault(t *testing.T) {
	tests := []struct {
		base       int
		configured int
		want       int
	}{
		{base: 4, configured: 0, want: 20},
		{base: 8, configured: 0, want: 32},
		{base: 32, configured: 0, want: 128},
		{base: 8, configured: 24, want: 24},
	}

	for _, tt := range tests {
		if got := effectiveMaxPoolSize(tt.base, tt.configured); got != tt.want {
			t.Fatalf("effectiveMaxPoolSize(%d, %d) = %d, want %d", tt.base, tt.configured, got, tt.want)
		}
	}
}
