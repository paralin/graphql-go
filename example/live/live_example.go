package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"context"
	"github.com/neelance/graphql-go"
	"github.com/neelance/graphql-go/response"
)

// To demonstrate the live capabilities,
// experiment with putting @defer on different fields.

var Schema string = `
type Region {
  id: String
  name: String
  status: Int
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

func (r *RootResolver) Regions() <-chan *RegionResolver {
	ch := make(chan *RegionResolver, 1)
	go func() {
		arr := []*RegionResolver{
			{
				id:    "test",
				name:  "test region",
				delay: time.Duration(0),
			},
			{
				id:     "test2",
				name:   "test region, delayed by 500ms",
				delay:  time.Duration(500) * time.Millisecond,
				doLive: true,
			},
			{
				id:    "test3",
				name:  "test region, delayed by 1sec",
				delay: time.Duration(1) * time.Second,
			},
		}

		for _, thing := range arr {
			ch <- thing
		}
		close(ch)
	}()
	return ch
}

type RegionResolver struct {
	id     string
	name   string
	delay  time.Duration
	doLive bool
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

func (r *RegionResolver) Status(ctx context.Context) <-chan int {
	ch := make(chan int, 1)

	go func() {
		done := ctx.Done()
		defer close(ch)
		i := 0
		for {
			i++
			select {
			case <-done:
				return
			case <-time.After(time.Duration(1) * time.Second):
				select {
				case <-done:
					return
				case ch <- i:
					if !r.doLive {
						return
					}
					if i >= 10 {
						return
					}
				}
			}
		}
	}()

	return ch
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
query regions {
  regions @stream {
    __typename
    id
    name @defer
    status @live @defer
  }
}
`

	fmt.Printf("Query:\n%s\n\nResponse stream:\n", query)

	ch := make(chan *response.Response, 1)
	resp := schema.ExecLive(context.Background(), query, "", make(map[string]interface{}), ch)

	dat, _ := json.Marshal(resp)
	fmt.Printf("%s\n", string(dat))

	for ent := range ch {
		dat, err = json.Marshal(ent)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
		} else {
			fmt.Printf("%s\n", string(dat))
		}
	}
	fmt.Printf("Done\n")
	return nil
}
