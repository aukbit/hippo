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

		store, err := clt.Dispatch(ctx, evt, &pb.User{})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		state := store.State.(*pb.User)

		// Write it back to the client.
		fmt.Fprintf(w, "hi %s!\n", state.GetName())
	})
}

func userRules(topic string, buffer, previous interface{}) (next interface{}) {
	switch topic {
	default:
		return previous
	case "user_created":
		return buffer
	}
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
	client.RegisterDomainRules(userRules, &pb.User{})

	// Register our handler.
	http.Handle("/hello", helloHandler(client))
	http.ListenAndServe(":8082", nil)
}
