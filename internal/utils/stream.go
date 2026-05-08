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

func Contains[T comparable](slice []T, item T) bool {
	for _, a := range slice {
		if a == item {
			return true
		}	
	}
	return false
}
