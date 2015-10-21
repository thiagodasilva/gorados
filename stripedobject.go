//  Copyright (c) 2015 Thiago da Silva
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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

	ret := C.rados_striper_read(rso.Striper, obj,
		(*C.char)(unsafe.Pointer(&p[0])), C.size_t(len(p)),
		C.uint64_t(rso.Read_offset))
	if ret < 0 {
		return int(ret), errors.New("read failed")
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

func (rso *RadosStripedObject) Setxattr(attr string, data []byte) (err error) {
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))

	c_attr := C.CString(attr)
	defer C.free(unsafe.Pointer(c_attr))

	c_data := (*C.char)(unsafe.Pointer(&data[0]))
	c_size := C.size_t(len(data))
	ret := C.rados_striper_setxattr(rso.Striper, obj, c_attr, c_data, c_size)
	if ret < 0 {
		return errors.New("Unable to write xattr")
	}
	return
}

func (rso *RadosStripedObject) Getxattr(attr string) (data []byte, err error) {
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))

	c_attr := C.CString(attr)
	defer C.free(unsafe.Pointer(c_attr))

	// TODO: this size is not enough
	buf := make([]byte, 4096)
	ret := C.rados_striper_getxattr(rso.Striper, obj, c_attr,
		(*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)))
	if ret < 0 {
		return nil, errors.New("get xattr failed")
	}
	return buf[:ret], nil
}

// Synchronously get object stats
// returns object size and modification time
func (rso *RadosStripedObject) Stat() (size uint64, pmtime uint64, err error) {
	obj := C.CString(rso.ObjectName)
	defer C.free(unsafe.Pointer(obj))

	var c_size C.uint64_t
	var c_time_t C.time_t
	ret := C.rados_striper_stat(rso.Striper, obj, &c_size, &c_time_t)
	if ret < 0 {
		return 0, 0, errors.New("get stat failed")
	}
	return uint64(c_size), uint64(C.uint64_t(c_time_t)), nil
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
