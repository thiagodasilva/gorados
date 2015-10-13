package main

// #cgo LDFLAGS: -lrados -lradosstriper
// #include <stdio.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
import "C"
import "unsafe"

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type RadosStripedObject struct {
	objectName string
	offset     int64
	striper    C.rados_striper_t
	ioctx      C.rados_ioctx_t
}

func (rso RadosStripedObject) WriteAt(p []byte, off int64) (n int, err error) {
	obj := C.CString(rso.objectName)
	defer C.free(unsafe.Pointer(obj))
	c_offset := C.uint64_t(off)
	c_data := (*C.char)(unsafe.Pointer(&p[0]))
	c_size := C.size_t(len(p))
	ret := C.rados_striper_write(rso.striper, obj, c_data, c_size, c_offset)
	if ret < 0 {
		return int(ret), errors.New("Unable to write")
	} else {
		return len(p), nil
	}
}

func (rso RadosStripedObject) Write2(p []byte) (n int, err error) {

	written, err := rso.WriteAt(p, rso.offset)
	if err != nil {
		return -1, err
	}
	rso.offset = rso.offset + int64(written)
	return written, nil
}

func (rso RadosStripedObject) Write(p []byte) (n int, err error) {
	obj := C.CString(rso.objectName)
	defer C.free(unsafe.Pointer(obj))

	c_data := (*C.char)(unsafe.Pointer(&p[0]))
	c_size := C.size_t(len(p))

	ret := C.rados_striper_append(rso.striper, obj, c_data, c_size)
	if ret < 0 {
		return int(ret), errors.New("Unable to write")
	} else {
		return len(p), nil
	}
}

func read_stripe(io C.rados_ioctx_t, key string, size int) []byte {
	var striper C.rados_striper_t

	err := C.rados_striper_create(io, &striper)
	if err < 0 {
		fmt.Println("create striper failed")
	}
	defer C.rados_striper_destroy(striper)
	obj := C.CString(key)

	read_data := make([]byte, size)
	err = C.rados_striper_read(striper, obj,
		(*C.char)(unsafe.Pointer(&read_data[0])), C.size_t(size), 0)
	if err < 0 {
		fmt.Println("read failed")
	}

	return read_data
}

func main() {
	var cluster C.rados_t
	conf := C.CString("/etc/ceph/ceph.conf")
	pool := C.CString("test")
	defer C.free(unsafe.Pointer(conf))
	defer C.free(unsafe.Pointer(pool))

	err := C.rados_create(&cluster, nil)
	if err < 0 {
		fmt.Println("create failed")
	}

	err1 := C.rados_conf_read_file(cluster, conf)
	if err1 < 0 {
		fmt.Println("conf failed")
	}

	err2 := C.rados_connect(cluster)
	if err2 < 0 {
		fmt.Println("connect failed")
	}
	defer C.rados_shutdown(cluster)

	obj := RadosStripedObject{}
	err = C.rados_ioctx_create(cluster, pool, &obj.ioctx)
	if err < 0 {
		fmt.Println("create ioctx failed")
	}
	defer C.rados_ioctx_destroy(obj.ioctx)

	ret := C.rados_striper_create(obj.ioctx, &obj.striper)
	if ret < 0 {
		fmt.Println("create striper failed")
	}
	defer C.rados_striper_destroy(obj.striper)

	f, e := os.Open("/home/vagrant/input.txt")
	if e != nil {
		panic(e)
	}
	obj.objectName = "obj"
	io.Copy(obj, f)

	read_data := read_stripe(obj.ioctx, obj.objectName, 21)
	fmt.Println(string(read_data))

}
