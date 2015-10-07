package main

// #cgo LDFLAGS: -lrados
// #include <stdio.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <rados/libradosstriper.h>
import "C"
import "unsafe"

import (
	"fmt"
)

func write_stripe(io C.rados_ioctx_t, key string, data string, size int) {
	var striper C.rados_striper_t

	err = C.rados_striper_create(io, &striper)
	if err < 0 {
		fmt.Println("create striper failed")
	} else {
		fmt.Println("create striper success")
	}
	defer C.rados_striper_destroy(striper)

	obj := C.CString(key)
	cdata := C.CString(data)
	err := C.rados_striper_write_full(striper, obj, cdata, C.size_t(size)) 
	if err < 0 {
		fmt.Println("stripe write failed")
	} else {
		fmt.Println("stripe write success")
	}

}

func write(io C.rados_ioctx_t, key string, data string, size int) {
	offset := 0
	stripe_size := 5
	stripe_count := size / stripe_size
	for i := 0; i < stripe_count; i++ {
		key_s := fmt.Sprint(key, "_", i)
		obj := C.CString(key_s)
		cdata := C.CString(data[offset:offset+stripe_size])
		data_size := C.size_t(stripe_size)
		defer C.free(unsafe.Pointer(obj))
		defer C.free(unsafe.Pointer(cdata))

		err := C.rados_write_full(io, obj, cdata, data_size)
		if err < 0 {
			fmt.Println("write failed")
		} else {
			fmt.Println("write success")
		}
		offset = offset + stripe_size
	}
}

func read(io C.rados_ioctx_t, key string, size int) string {
	i := 0
	key_s := fmt.Sprint(key, "_", i)
	c_obj := C.CString(key_s)
	defer C.free(unsafe.Pointer(c_obj))
	read_data := make([]byte, size)
	err := C.rados_read(io, c_obj, (*C.char)(unsafe.Pointer(&read_data[0])), C.size_t(size), 0)	
	if err < 0 {
		fmt.Println("read failed")
	} else {
		fmt.Println("read success")
	}
	return string(read_data)
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

	
        godata := "hello librados"
	obj := "obj"
	write_stripe(io, obj, godata, len(godata)) 
	read_data := read(io, obj, len(godata))

	fmt.Println(read_data)
	C.rados_ioctx_destroy(io)

	C.rados_shutdown(cluster)
}
