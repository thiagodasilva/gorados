package main

// #cgo LDFLAGS: -lrados -lradosstriper
// #include <stdio.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
import "C"
import "unsafe"

import (
	"fmt"
)


func rados_write_stripe(striper C.rados_striper_t, obj_name string, data []byte, offset int) int {
	//fmt.Println("writing this data:", string(data))
	obj := C.CString(obj_name)
	defer C.free(unsafe.Pointer(obj))
	c_offset := C.uint64_t(offset)
	c_data := (*C.char)(unsafe.Pointer(&data[0]))
	c_size := C.size_t(len(data))
	err := C.rados_striper_write(striper, obj, c_data, c_size, c_offset)
	return int(err)
}
	
func write_stripe(io C.rados_ioctx_t, obj_name string, data []byte) {
	var striper C.rados_striper_t

	err := C.rados_striper_create(io, &striper)
	if err < 0 {
		fmt.Println("create striper failed")
	} else {
		fmt.Println("create striper success")
	}
	defer C.rados_striper_destroy(striper)

	written := 0
	buf_len := 0
	buf_size := 1024*64
	for written < len(data) {
		buf_len = written+buf_size
		if buf_len > len(data) {
			buf_len = len(data)
		}
		err := rados_write_stripe(striper, obj_name, data[written:buf_len], written)
		if err < 0 {
			fmt.Println("stripe write failed")
			return
		}
		written += buf_size	
	}

}

func read_stripe(io C.rados_ioctx_t, key string, size int) []byte {
	var striper C.rados_striper_t

	err := C.rados_striper_create(io, &striper)
	if err < 0 {
		fmt.Println("create striper failed")
	} else {
		fmt.Println("create striper success")
	}
	defer C.rados_striper_destroy(striper)
	obj := C.CString(key)

	read_data := make([]byte, size)
	err = C.rados_striper_read(striper, obj, (*C.char)(unsafe.Pointer(&read_data[0])), C.size_t(size), 0)	
	if err < 0 {
		fmt.Println("read failed")
	} else {
		fmt.Println("read success")
	}

	return read_data
}

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

	
    	godata := make([]byte, 50000000)
	for i := range godata { godata[i] = 1 }
	fmt.Println(godata[:10])
	//godata := []byte("hello world of golang")
	obj := "obj"
	write_stripe(io, obj, godata) 

	read_data := read_stripe(io, obj, 10)
	fmt.Println(read_data)

	C.rados_ioctx_destroy(io)

	C.rados_shutdown(cluster)
}
