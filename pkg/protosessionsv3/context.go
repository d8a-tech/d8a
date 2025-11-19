package protosessionsv3

import (
	"context"
	"fmt"
)

func CtxSetValue[T any](ctx context.Context, key any, value T) context.Context {
	return context.WithValue(ctx, key, value)
}

func CtxGetValue[T any](ctx context.Context, key any) (T, bool) {
	value, ok := ctx.Value(key).(T)
	return value, ok
}

func CtxMustValue[T any](ctx context.Context, key any) T {
	value, ok := CtxGetValue[T](ctx, key)
	if !ok {
		panic(fmt.Sprintf("value not found for key: %v", key))
	}
	return value
}
