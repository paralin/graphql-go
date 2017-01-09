package response

import (
	"github.com/neelance/graphql-go/errors"
)

type Response struct {
	Data       interface{}            `json:"data,omitempty"`
	Errors     []*errors.QueryError   `json:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

func ConstructLiveResponse(path []interface{}, data interface{}, errors []*errors.QueryError) *Response {
	return &Response{
		Data:   data,
		Errors: errors,
		Extensions: map[string]interface{}{
			"path": path,
		},
	}
}
