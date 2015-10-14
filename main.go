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
	objectName   string
	striper      C.rados_striper_t
	ioctx        C.rados_ioctx_t
	read_offset  int64
	write_offset int64
}

// Synchronously removes a striped object
func (rso *RadosStripedObject) Remove() (err error) {
	obj := C.CString(rso.objectName)
	defer C.free(unsafe.Pointer(obj))
	ret, err := C.rados_striper_remove(rso.striper, obj)
	if ret < 0 {
		return err
	} else {
		return nil
	}
}

// Resize an object
// TODO: doesn't work! call to rados_striper_trunc fails
func (rso *RadosStripedObject) Truncate(size int64) (err error) {

	obj := C.CString(rso.objectName)
	defer C.free(unsafe.Pointer(obj))
	c_size := C.uint64_t(size)
	ret, err := C.rados_striper_trunc(rso.ioctx, obj, c_size)
	if ret < 0 {
		return err
	} else {
		return nil
	}
	return
}

// Write data to a striped object at the specified offset
// Implements the WriterAt interface
func (rso *RadosStripedObject) WriteAt(p []byte, off int64) (n int, err error) {
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

// Write data to a striped object
// Implements the Writer interface
func (rso *RadosStripedObject) Write(p []byte) (n int, err error) {
	written, err := rso.WriteAt(p, rso.write_offset)
	if err != nil {
		return -1, err
	}
	rso.write_offset = rso.write_offset + int64(written)
	return written, nil
}

// Read data from a striped object
func (rso *RadosStripedObject) Read(p []byte) (n int, err error) {
	obj := C.CString(rso.objectName)
	defer C.free(unsafe.Pointer(obj))

	ret, err := C.rados_striper_read(rso.striper, obj,
		(*C.char)(unsafe.Pointer(&p[0])), C.size_t(len(p)),
		C.uint64_t(rso.read_offset))
	if ret < 0 {
		return int(ret), err
	} else {
		// update offset value for next read
		rso.read_offset = rso.read_offset + int64(ret)

		// if actual read was less than buffer size than reached end of object
		if int(ret) < len(p) {
			err = io.EOF
		} else {
			err = nil
		}
		return int(ret), err
	}
}

// test
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

	obj := &RadosStripedObject{}
	obj.objectName = "obj"

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

	// clean up any existing object with same name
	// TODO: ideally would use truncate here
	e = obj.Remove()
	if e != nil {
		panic(e)
	}

	// write to librados object
	io.Copy(obj, f)

	out, e := os.Create("/home/vagrant/output.txt")
	if e != nil {
		panic(e)
	}

	// read from librados object
	io.Copy(out, obj)

}
