package graphql

import (
	"context"
	"encoding/json"
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"github.com/neelance/graphql-go/errors"
	"github.com/neelance/graphql-go/internal/exec"
	"github.com/neelance/graphql-go/internal/query"
	"github.com/neelance/graphql-go/internal/schema"
	"github.com/neelance/graphql-go/introspection"
	"github.com/neelance/graphql-go/response"
)

const OpenTracingTagQuery = "graphql.query"
const OpenTracingTagOperationName = "graphql.operationName"
const OpenTracingTagVariables = "graphql.variables"

const OpenTracingTagType = exec.OpenTracingTagType
const OpenTracingTagField = exec.OpenTracingTagField
const OpenTracingTagTrivial = exec.OpenTracingTagTrivial
const OpenTracingTagArgsPrefix = exec.OpenTracingTagArgsPrefix
const OpenTracingTagError = exec.OpenTracingTagError

type ID string

func (_ ID) ImplementsGraphQLType(name string) bool {
	return name == "ID"
}

func (id *ID) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case string:
		*id = ID(input)
		return nil
	default:
		return fmt.Errorf("wrong type")
	}
}

func ParseSchema(schemaString string, resolver interface{}) (*Schema, error) {
	s := &Schema{
		schema: schema.New(),
	}
	if err := s.schema.Parse(schemaString); err != nil {
		return nil, err
	}

	if resolver != nil {
		e, err := exec.Make(s.schema, resolver)
		if err != nil {
			return nil, err
		}
		s.exec = e
	}

	return s, nil
}

func MustParseSchema(schemaString string, resolver interface{}) *Schema {
	s, err := ParseSchema(schemaString, resolver)
	if err != nil {
		panic(err)
	}
	return s
}

type Schema struct {
	schema *schema.Schema
	exec   *exec.Exec
}

func (s *Schema) ExecLive(ctx context.Context, queryString string, operationName string, variables map[string]interface{}, liveChannel chan<- *response.Response) *response.Response {
	if s.exec == nil {
		panic("schema created without resolver, can not exec")
	}

	document, err := query.Parse(queryString, s.schema.Resolve)
	if err != nil {
		return &response.Response{
			Errors: []*errors.QueryError{err},
		}
	}

	spanName := "GraphQL request"
	if liveChannel != nil {
		spanName = "GraphQL Live Request"
	}

	span, subCtx := opentracing.StartSpanFromContext(ctx, spanName)
	span.SetTag(OpenTracingTagQuery, queryString)
	if operationName != "" {
		span.SetTag(OpenTracingTagOperationName, operationName)
	}
	if len(variables) != 0 {
		span.SetTag(OpenTracingTagVariables, variables)
	}

	data, liveWg, errs := exec.ExecuteRequest(subCtx, s.exec, document, operationName, variables, liveChannel)
	if len(errs) != 0 {
		ext.Error.Set(span, true)
		span.SetTag(OpenTracingTagError, errs)
	}
	if liveChannel == nil || liveWg == nil {
		defer span.Finish()
	} else {
		go func() {
			liveWg.Wait()
			span.Finish()
		}()
	}
	return &response.Response{
		Data:   data,
		Errors: errs,
	}
}

func (s *Schema) Exec(ctx context.Context, queryString string, operationName string, variables map[string]interface{}) *response.Response {
	return s.ExecLive(ctx, queryString, operationName, variables, nil)
}

func (s *Schema) Inspect() *introspection.Schema {
	return &introspection.Schema{Schema: s.schema}
}

func (s *Schema) ToJSON() ([]byte, error) {
	result, err := exec.IntrospectSchema(s.schema)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(result, "", "\t")
}
