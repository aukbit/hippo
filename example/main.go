package main

import (
	"fmt"
	"net/http"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/influxdb"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

func helloHandler(clt *hippo.Client) http.Handler {
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

		rules := func(e *hippo.Event, currentState, nextState hippo.Data) error {
			if e.Schema == fmt.Sprintf("%T", nextState) {
				if err := e.UnmarshalProto(nextState); err != nil {
					return err
				}
			}
			switch e.Topic {
			default:
				nextState = currentState
			case "user_created":
				return nil
			}
			return nil
		}

		domain := hippo.Domain{NextState: &pb.User{}, Rules: rules}

		store, err := clt.Dispatch(r.Context(), msg, domain)
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
	store := influxdb.NewStoreService()
	if err := store.Connect(influxdb.Config{
		Database: "hippo_example_db",
	}); err != nil {
		panic(err)
	}

	client := hippo.NewClient(store)

	// Register our handler.
	http.Handle("/hello", helloHandler(client))
	http.ListenAndServe(":8082", nil)
}
