package gorados

// #cgo LDFLAGS: -lrados -lradosstriper
// #include <stdio.h>
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
import "C"

import (
	"errors"
	"fmt"
	"io"
	"unsafe"
)

type RadosCluster struct {
	Cluster C.rados_t
	Ioctx   C.rados_ioctx_t
}

type RadosStripedObject struct {
	ObjectName   string
	Striper      C.rados_striper_t
	Ioctx        C.rados_ioctx_t
	Read_offset  int64
	Write_offset int64
}

// Synchronously removes a striped object
func (rso *RadosStripedObject) Remove() (err error) {
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))
	ret := C.rados_striper_remove(rso.Striper, obj)
	if ret < 0 && ret != -2 {
		fmt.Println("removing object failed", ret, err)
		return err
	} else {
		return nil
	}
}

// Resize an object
// TODO: doesn't work! call to rados_striper_trunc failGs
func (rso *RadosStripedObject) Truncate(size int64) (err error) {

	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))
	c_size := C.uint64_t(size)
	ret, err := C.rados_striper_trunc(rso.Ioctx, obj, c_size)
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
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))
	c_offset := C.uint64_t(off)
	c_data := (*C.char)(unsafe.Pointer(&p[0]))
	c_size := C.size_t(len(p))
	ret := C.rados_striper_write(rso.Striper, obj, c_data, c_size, c_offset)
	if ret < 0 {
		return int(ret), errors.New("Unable to write")
	} else {
		return len(p), nil
	}
}

// Write data to a striped object
// Implements the Writer interface
func (rso *RadosStripedObject) Write(p []byte) (n int, err error) {
	written, err := rso.WriteAt(p, rso.Write_offset)
	if err != nil {
		return -1, err
	}
	rso.Write_offset = rso.Write_offset + int64(written)
	return written, nil
}

// Read data from a striped object
func (rso *RadosStripedObject) Read(p []byte) (n int, err error) {
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))

	ret, err := C.rados_striper_read(rso.Striper, obj,
		(*C.char)(unsafe.Pointer(&p[0])), C.size_t(len(p)),
		C.uint64_t(rso.Read_offset))
	if ret < 0 {
		return int(ret), err
	} else {
		// update offset value for next read
		rso.Read_offset = rso.Read_offset + int64(ret)

		// if actual read was less than buffer size than reached end of object
		if int(ret) < len(p) {
			err = io.EOF
		} else {
			err = nil
		}
		return int(ret), err
	}
}

func (rso *RadosStripedObject) Connect() (err error) {
	ret := C.rados_striper_create(rso.Ioctx, &rso.Striper)
	if ret < 0 {
		return errors.New("create striper failed")
	}
	return
}

func (rso *RadosStripedObject) Destroy() (err error) {
	C.rados_striper_destroy(rso.Striper)
	return
}

func (rc *RadosCluster) Connect(conf string, pool string) (err error) {
	c_conf := C.CString(conf)
	c_pool := C.CString(pool)
	defer C.free(unsafe.Pointer(c_conf))
	defer C.free(unsafe.Pointer(c_pool))

	ret := C.rados_create(&rc.Cluster, nil)
	if ret < 0 {
		return errors.New("create cluster failed")
	}

	ret = C.rados_conf_read_file(rc.Cluster, c_conf)
	if ret < 0 {
		return errors.New("read conf file failed")
	}

	ret = C.rados_connect(rc.Cluster)
	if ret < 0 {
		return errors.New("connect to cluster failed")
	}

	ret = C.rados_ioctx_create(rc.Cluster, c_pool, &rc.Ioctx)
	if ret < 0 {
		return errors.New("create ioctx failed")
	}
	return nil
}

func (rc *RadosCluster) Close() (err error) {
	C.rados_ioctx_destroy(rc.Ioctx)
	return nil
}
