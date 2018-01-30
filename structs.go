// auxilary structures for replicagor
package main

import (
	"container/list"
)

type schema struct {
	name   string
	tables []table
}

type table struct {
	name string

	fieldIds []int
}

type queue struct {
	l list.List
}
