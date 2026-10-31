package columns

import "github.com/d8a-tech/d8a/pkg/schema"

// QueryParamSource is implemented by event columns that read exactly one hit query parameter.
type QueryParamSource interface {
	SourceQueryParam() string
}

// PageURLParamSource is implemented by event columns that read one query parameter of the page URL.
type PageURLParamSource interface {
	SourcePageURLParam() string
}

// EventColumnUnwrapper is implemented by wrappers that decorate another event column.
type EventColumnUnwrapper interface {
	Unwrap() schema.EventColumn
}

// EventColumnSources returns the hit query parameter and page URL parameter a column reads,
// looking through wrappers. Empty strings mean the column has no single-parameter source.
func EventColumnSources(column schema.EventColumn) (queryParam, pageURLParam string) {
	for column != nil {
		if source, ok := column.(QueryParamSource); ok && source.SourceQueryParam() != "" {
			return source.SourceQueryParam(), ""
		}
		if source, ok := column.(PageURLParamSource); ok && source.SourcePageURLParam() != "" {
			return "", source.SourcePageURLParam()
		}
		wrapper, ok := column.(EventColumnUnwrapper)
		if !ok {
			return "", ""
		}
		column = wrapper.Unwrap()
	}
	return "", ""
}
