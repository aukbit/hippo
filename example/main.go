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

		ctx := r.Context()

		user := pb.User{
			Id:    rand.String(10),
			Name:  "example",
			Email: "example@email.com",
		}

		// Create new event for user_created topic.
		evt := hippo.NewEventProto("user_created", user.GetId(), &user)

		rules := func(topic string, old, new interface{}) interface{} {
			switch topic {
			default:
				return old
			case "user_created":
				return new
			}
		}

		domain := hippo.Domain{Output: &pb.User{}, Rules: rules}

		store, err := clt.Dispatch(ctx, evt, domain)
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
