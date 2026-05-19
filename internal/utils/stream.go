package utils

import (
	"iter"
)

func MapSlice[T any, U any](slice []T, f func(T) U) []U {
	result := make([]U, len(slice))
	for i, v := range slice {
		result[i] = f(v)
	}
	return result
}

func MapIter[T any, U any](seq iter.Seq[T], f func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for a := range seq {
			if !yield(f(a)) {
				return
			}
		}
	}
}

func FilterSlice[T any](slice []T, f func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for _, v := range slice {
		if f(v) {
			result = append(result, v)
		}
	}
	return result
}

func FilterIter[T any](seq iter.Seq[T], f func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		for a := range seq {
			if f(a) && !yield(a) {
				return
			}
		}
	}
}
