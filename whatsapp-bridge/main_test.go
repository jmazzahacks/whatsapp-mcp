package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStagedMediaRetainsPathRestrictions(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("store/ff-send-test", 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"store/ff-send-test/report.pdf", "store/whatsapp.db", "store/ff-send-test/session.db", "outside.pdf"} {
		if err := os.WriteFile(name, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	outside, err := filepath.Abs("outside.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, "store/ff-send-test/escape.pdf"); err != nil {
		t.Fatal(err)
	}
	if err := checkSendableMediaPath("store/ff-send-test/report.pdf"); err != nil {
		t.Fatalf("staged attachment rejected: %v", err)
	}
	for _, name := range []string{"store/whatsapp.db", "store/ff-send-test/session.db", "outside.pdf", "store/ff-send-test/escape.pdf"} {
		if err := checkSendableMediaPath(name); err == nil {
			t.Errorf("unsafe path accepted: %s", name)
		}
	}
}
