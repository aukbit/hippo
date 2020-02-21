package main

import (
	"fmt"
	"net/http"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/influxdb"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

func helloHandler(clt hippo.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		user := pb.User{
			Id:    rand.String(10),
			Name:  "example",
			Email: "example@email.com",
		}

		// Create new event for user_created topic.
		ev1 := hippo.NewEvent("user_created", user.GetId(), nil)
		// Marshal user proto and assign it to event data
		if err := ev1.MarshalProto(&user); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		msg := hippo.Message{
			ID:    user.GetId(),
			Event: ev1,
		}

		rules := func(e *hippo.Event, state interface{}) (interface{}, error) {
			n := pb.User{}
			if e.Schema == fmt.Sprintf("%T", &pb.User{}) {
				if err := e.UnmarshalProto(&n); err != nil {
					return nil, err
				}
			}
			switch e.Topic {
			default:
				return state, nil
			case "user_created":
				return &n, nil
			}
			return state, nil
		}

		store, err := clt.Dispatch(r.Context(), msg, rules)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		state := store.State.(*pb.User)

		// Write it back to the client.
		fmt.Fprintf(w, "hi %s!\n", state.GetName())
	})
}

func main() {
	// Open our database connection.
	c := influxdb.NewClient()
	if err := c.Connect(influxdb.Config{
		Database: "hippo_example_db",
	}); err != nil {
		panic(err)
	}

	// Register our handler.
	http.Handle("/hello", helloHandler(c))
	http.ListenAndServe(":8080", nil)
}
