package newstore

import (
	"testing"
)

func TestGreeting(t *testing.T) {
	expected := "Hello, NewStore!"

	actual := Hello("NewStore")

	if actual != expected {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
