package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/neelance/graphql-go"
	"github.com/neelance/graphql-go/response"
	"golang.org/x/net/context"
)

// To demonstrate the live capabilities,
// experiment with putting @defer on different fields.

var Schema string = `
type Region {
  id: String
  name: String
}

type RootQuery {
  regions: [Region]
}

schema {
  query: RootQuery
}
`

type RootResolver struct {
}

func (r *RootResolver) Regions() *[]*RegionResolver {
	return &[]*RegionResolver{
		{
			id:    "test",
			name:  "test region",
			delay: time.Duration(0),
		},
		{
			id:    "test2",
			name:  "test region, delayed by 500ms",
			delay: time.Duration(500) * time.Millisecond,
		},
		{
			id:    "test3",
			name:  "test region, delayed by 1sec",
			delay: time.Duration(1) * time.Second,
		},
	}
}

type RegionResolver struct {
	id    string
	name  string
	delay time.Duration
}

func (r *RegionResolver) Id() *string {
	return &r.id
}

func (r *RegionResolver) Name() *string {
	// Note: if you don't sleep here,
	// GraphQL-Go will be intelligent, and there's a good chance
	// it will send the field in the initial response,
	// even if it is @deferred.
	// Since the resolver returns instantly,
	// GraphQL-Go detects there's no advantage to deferring it.
	time.Sleep(r.delay)
	return &r.name
}

func main() {
	if err := realMain(); err != nil {
		fmt.Printf("Error: %v\n", err.Error())
		os.Exit(1)
	}
}

func realMain() error {
	schema, err := graphql.ParseSchema(Schema, &RootResolver{})
	if err != nil {
		return err
	}
	query := `
fragment regionDetails on Region {
  name
}

query regions {
  regions @stream {
    __typename
    id
    ...regionDetails @defer
  }
}
`

	ch := make(chan *response.Response, 1)
	resp := schema.ExecLive(context.Background(), query, "", make(map[string]interface{}), ch)

	dat, _ := json.Marshal(resp)
	fmt.Printf("Initial: %s\n", string(dat))

	for ent := range ch {
		dat, _ = json.Marshal(ent)
		fmt.Printf("Live: %s\n", string(dat))
	}
	fmt.Printf("Done\n")
	return nil
}
