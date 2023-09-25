package cli

import "testing"

func TestParseParams(t *testing.T) {
	tests := []struct {
		input                            []string
		profile, region, table, endpoint string
	}{
		{
			input:    []string{"--profile", "myProfile", "--region", "us-west-1", "--table", "myTable", "--endpoint-url", "http://localhost:8000"},
			profile:  "myProfile",
			region:   "us-west-1",
			table:    "myTable",
			endpoint: "http://localhost:8000",
		},
		{
			input:    []string{"--profile", "myProfile", "--region", "us-west-1"},
			profile:  "myProfile",
			region:   "us-west-1",
			table:    "",
			endpoint: "",
		},
		{
			input:    []string{"--table", "myTable", "--endpoint-url", "http://localhost:8000"},
			profile:  "",
			region:   "",
			table:    "myTable",
			endpoint: "http://localhost:8000",
		},
	}

	for _, tt := range tests {
		profile, region, table, endpoint := parseParams(tt.input)
		if profile != tt.profile {
			t.Errorf("Expected profile %s, got %s", tt.profile, profile)
		}
		if region != tt.region {
			t.Errorf("Expected region %s, got %s", tt.region, region)
		}
		if table != tt.table {
			t.Errorf("Expected table %s, got %s", tt.table, table)
		}
		if endpoint != tt.endpoint {
			t.Errorf("Expected endpoint %s, got %s", tt.endpoint, endpoint)
		}
	}
}
