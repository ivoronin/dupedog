package main

import (
	"testing"
)

// =============================================================================
// Section 7.1: CLI Utility Tests (parseSize)
// =============================================================================

// TestParseSizeValid tests valid size strings.
// Note: humanize.ParseBytes uses SI units (decimal) for KB/MB/GB (1000-based)
// and IEC units (binary) for KiB/MiB/GiB (1024-based).
func TestParseSizeValid(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		// SI units (decimal, 1000-based)
		{"1k", 1000},
		{"1K", 1000},
		{"1kb", 1000},
		{"1KB", 1000},
		{"1m", 1000000},
		{"1M", 1000000},
		{"1mb", 1000000},
		{"1MB", 1000000},
		{"1g", 1000000000},
		{"1G", 1000000000},
		{"1gb", 1000000000},
		{"1GB", 1000000000},

		// No suffix (bytes)
		{"1234", 1234},
		{"0", 0},

		// Larger SI values
		{"100k", 100000},
		{"10m", 10000000},
		{"2g", 2000000000},

		// IEC suffixes (binary, 1024-based)
		{"1KiB", 1024},
		{"1MiB", 1048576},
		{"1GiB", 1073741824},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize(tt.input)
			if err != nil {
				t.Fatalf("parseSize(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Section 7.2: CLI Validation Edge Cases (CRITICAL)
// =============================================================================

// TestParseSizeInvalid tests invalid size strings.
func TestParseSizeInvalid(t *testing.T) {
	tests := []string{
		"invalid",
		"abc",
		"1.5.5",
		"--100",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := parseSize(input)
			if err == nil {
				t.Errorf("parseSize(%q) should return error", input)
			}
		})
	}
}

// TestParseSizeNegative tests that negative values are rejected.
func TestParseSizeNegative(t *testing.T) {
	negatives := []string{"-1", "-1k", "-100M", "-0"}
	for _, s := range negatives {
		t.Run(s, func(t *testing.T) {
			_, err := parseSize(s)
			if err == nil {
				t.Errorf("parseSize(%q) should return error for negative value", s)
			}
		})
	}
}

// TestParseSizeFloatingPoint tests that floating point values are supported.
func TestParseSizeFloatingPoint(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1.5M", 1500000},
		{"0.5K", 500},
		{"2.5G", 2500000000},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize(tt.input)
			if err != nil {
				t.Fatalf("parseSize(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// TestParseSizeOverflow tests that very large values are rejected.
func TestParseSizeOverflow(t *testing.T) {
	overflows := []string{"999999999999999999T", "99999999999999999999"}
	for _, s := range overflows {
		t.Run(s, func(t *testing.T) {
			_, err := parseSize(s)
			if err == nil {
				t.Errorf("parseSize(%q) should return error for overflow value", s)
			}
		})
	}
}

// TestParseSizeZeroVariants tests various zero representations.
func TestParseSizeZeroVariants(t *testing.T) {
	variants := []string{"0", "0k", "0M", "0G"}
	for _, v := range variants {
		t.Run(v, func(t *testing.T) {
			got, err := parseSize(v)
			if err != nil {
				t.Fatalf("parseSize(%q) error: %v", v, err)
			}
			if got != 0 {
				t.Errorf("parseSize(%q) = %d, want 0", v, got)
			}
		})
	}
}

// TestParseSizeEmptyStringReturnsError tests that empty string is rejected.
func TestParseSizeEmptyStringReturnsError(t *testing.T) {
	_, err := parseSize("")
	if err == nil {
		t.Error("parseSize(\"\") should return error, got nil")
	}
}

// TestParseSizeTerabyte tests terabyte parsing.
func TestParseSizeTerabyte(t *testing.T) {
	got, err := parseSize("1T")
	if err != nil {
		t.Fatalf("parseSize(1T) error: %v", err)
	}
	want := int64(1000000000000) // 1 TB (SI, decimal)
	if got != want {
		t.Errorf("parseSize(1T) = %d, want %d", got, want)
	}

	// Also test TiB (binary)
	got, err = parseSize("1TiB")
	if err != nil {
		t.Fatalf("parseSize(1TiB) error: %v", err)
	}
	want = int64(1099511627776) // 1 TiB (IEC, binary)
	if got != want {
		t.Errorf("parseSize(1TiB) = %d, want %d", got, want)
	}
}

// =============================================================================
// Section 7.3: Glob Pattern Validation Tests
// =============================================================================

// TestValidateGlobPatternsValid tests valid patterns are accepted.
func TestValidateGlobPatternsValid(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{"single wildcard", []string{"*.txt"}},
		{"multiple patterns", []string{"*.txt", "*.bak", "temp*"}},
		{"question mark", []string{"file?.txt"}},
		{"character class", []string{"[abc].txt"}},
		{"empty slice", []string{}},
		{"nil slice", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGlobPatterns(tt.patterns)
			if err != nil {
				t.Errorf("validateGlobPatterns(%v) unexpected error: %v", tt.patterns, err)
			}
		})
	}
}

// TestValidateGlobPatternsInvalid tests invalid patterns are rejected.
func TestValidateGlobPatternsInvalid(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{"unclosed bracket", []string{"[invalid"}},
		{"mixed valid and invalid", []string{"*.txt", "[invalid"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGlobPatterns(tt.patterns)
			if err == nil {
				t.Errorf("validateGlobPatterns(%v) expected error, got nil", tt.patterns)
			}
		})
	}
}
