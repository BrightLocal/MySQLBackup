package mylogin_reader

import (
	"log"
	"testing"
)

func TestRead2(t *testing.T) {
	r := Read()
	if r.fileName == "" {
		t.Errorf("Could not detect any MySQL credentials file")
	}
	dsn, err := r.GetDSN("backup")
	if err != nil {
		t.Errorf("Failed reading from %q: %s", r.fileName, err)
	}
	log.Printf("DSN from %s: %q", r.fileName, dsn)
}
