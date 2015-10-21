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
	"unsafe"
)

type RadosCluster struct {
	Cluster C.rados_t
	Ioctx   C.rados_ioctx_t
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
