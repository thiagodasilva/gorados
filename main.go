package main

// #cgo LDFLAGS: -lrados
// #include <stdio.h>
// #include <stdlib.h>
// #include <rados/librados.h>
import "C"
import "unsafe"

import (
	"fmt"
)

func main() {
	var cluster C.rados_t
	var io C.rados_ioctx_t
	conf := C.CString("/home/vagrant/ceph.conf")
	pool := C.CString("test")
	defer C.free(unsafe.Pointer(conf))
	defer C.free(unsafe.Pointer(pool))

	err := C.rados_create(&cluster, nil)
	if err < 0 {
		fmt.Println("create failed")
	} else {
		fmt.Println("create success")
	}

	err1 := C.rados_conf_read_file(cluster, conf)
	if err1 < 0 {
		fmt.Println("conf failed")
	} else {
		fmt.Println("conf success")
	}

	err2 := C.rados_connect(cluster)
	if err2 < 0 {
		fmt.Println("connect failed")
	} else {
		fmt.Println("connect success")
	}

	err = C.rados_ioctx_create(cluster, pool, &io)
	if err < 0 {
		fmt.Println("create ioctx failed")
	} else {
		fmt.Println("create ioctx success")
	}

	obj := C.CString("hw")
        godata := "hello librados"
	data := C.CString(godata)
	data_size := C.size_t(len(godata))
	defer C.free(unsafe.Pointer(obj))
	defer C.free(unsafe.Pointer(data))
	
	err = C.rados_write_full(io, obj, data, data_size)
	if err < 0 {
		fmt.Println("write failed")
	} else {
		fmt.Println("write success")
	}

	read_data := make([]byte, 5)
	err = C.rados_read(io, obj, (*C.char)(unsafe.Pointer(&read_data[0])), 5, 0)	
	if err < 0 {
		fmt.Println("read failed")
	} else {
		fmt.Println("read success")
	}
	fmt.Println(string(read_data))
	C.rados_ioctx_destroy(io)

	C.rados_shutdown(cluster)
}
