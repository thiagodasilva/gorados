package main

// #cgo LDFLAGS: -lrados
// #include <rados/librados.h>
import "C"

import (
	"fmt"
)

func main() {
	var cluster C.rados_t

	err := C.rados_create(&cluster, nil)
	if err < 0 {
		fmt.Println("failed")
	} else {
		fmt.Println("success")
	}
}
