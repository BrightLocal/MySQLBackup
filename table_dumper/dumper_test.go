package table_dumper

import (
	"testing"
	"bytes"
)

func TestWriteHeader(t *testing.T) {
	b0 := &bytes.Buffer{}
	d0 := Dumper{
		w: b0,
	}
	if err := d0.writeHeader([]string{}); err != nil {
		t.Error(err)
	}
	if out := b0.String(); out != "" {
		t.Errorf("Got %q", out)
	}
	b1 := &bytes.Buffer{}
	d1 := Dumper{
		w: b1,
	}
	if err := d1.writeHeader([]string{"hello"}); err != nil {
		t.Error(err)
	}
	if out := b1.String(); out != "`hello`\n" {
		t.Errorf("Got %q", out)
	}
	b2 := &bytes.Buffer{}
	d2 := Dumper{
		w: b2,
	}
	if err := d2.writeHeader([]string{"hello", "world", "foo", "bar"}); err != nil {
		t.Error(err)
	}
	if out := b2.String(); out != "`hello`,`world`,`foo`,`bar`\n" {
		t.Errorf("Got %q", out)
	}
}
