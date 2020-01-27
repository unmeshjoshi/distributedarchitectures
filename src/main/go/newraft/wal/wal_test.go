package wal

import (
	"bytes"
	"consensus/newraft/pbutil"
	"consensus/newraft/wal/walpb"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(zap.NewExample(), p, []byte("somedata"))
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if g := filepath.Base(w.tail().Name()); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	defer w.Close()

	// file is preallocated to segment size; only read data written by wal
	off, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	gd := make([]byte, off)
	f, err := os.Open(filepath.Join(p, filepath.Base(w.tail().Name())))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = io.ReadFull(f, gd); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	var wb bytes.Buffer
	e := newEncoder(&wb, 0, 0)
	err = e.encode(&walpb.Record{Type: crcType, Crc: 0})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	err = e.encode(&walpb.Record{Type: metadataType, Data: []byte("somedata")})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	r := &walpb.Record{
		Type: snapshotType,
		Data: pbutil.MustMarshal(&walpb.Snapshot{}),
	}
	if err = e.encode(r); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	e.flush()


	d := &walpb.Record{}
	decoder := newDecoder(w.dirFile)
	decoder.decode(d)

	if !bytes.Equal(gd, wb.Bytes()) {
		t.Errorf("data = %v, want %v", gd, wb.Bytes())
	}
}