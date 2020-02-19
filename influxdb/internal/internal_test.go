package internal_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/influxdb/internal"
)

// Ensure event can be marshaled and unmarshaled.
func TestMarshalEvent(t *testing.T) {
	e := hippo.NewEvent("event_created", "123ABC", map[string]string{"eid": "12345"})
	e.Version = 1
	e.Schema = "string"
	e.Format = hippo.STRING
	e.Data = []byte("Hello World!")
	e.Priority = 1
	e.Signature = "Signature"
	e.OriginName = "Origin name"
	e.OriginIP = "Origin IP"
	e.CreateTime = time.Now().UTC()

	var other hippo.Event
	if buf, err := internal.MarshalEvent(e); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalEvent(buf, &other); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, &other) {
		t.Fatalf("unexpected copy: %#v != %#v", e, &other)
	}
}

func TestMarshalEventText(t *testing.T) {
	e := hippo.NewEvent("event_created", "123ABC", map[string]string{"eid": "12345"})
	e.Version = 1
	e.Schema = "string"
	e.Format = hippo.STRING
	e.Data = []byte("Hello World!")
	e.Priority = 1
	e.Signature = "Signature"
	e.OriginName = "Origin name"
	e.OriginIP = "Origin IP"
	e.CreateTime = time.Now().UTC()

	var other hippo.Event
	if str, err := internal.MarshalEventText(e); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalEventText(str, &other); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(e, &other) {
		t.Fatalf("unexpected copy: %#v != %#v", e, &other)
	}
}
