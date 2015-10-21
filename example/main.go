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

package main

import (
	"fmt"
	"github.com/thiagodasilva/gorados"
	"io"
	"os"
)

// test
func main() {
	conf := "/etc/ceph/ceph.conf"
	pool := "test"

	cluster := &gorados.RadosCluster{}
	cluster.Connect(conf, pool)
	obj := &gorados.RadosStripedObject{}
	obj.ObjectName = "obj"
	obj.Ioctx = cluster.Ioctx

	f, e := os.Open("/home/vagrant/input.txt")
	if e != nil {
		panic(e)
	}

	e = obj.Connect()
	if e != nil {
		panic(e)
	}

	// clean up any existing object with same name
	// TODO: ideally would use truncate here
	ee := obj.Remove()
	if ee != nil {
		panic(ee)
	}

	// write to librados object
	io.Copy(obj, f)
	f.Close()

	obj.Setxattr("user.test", []byte("testing setting xattr"))
	xattr_value, err := obj.Getxattr("user.test")
	if err != nil {
		panic(e)
	}
	fmt.Println("xattr_value: ", string(xattr_value))

	/*out, e := os.Create("/home/vagrant/output.txt")
	if e != nil {
		panic(e)
	}*/

	// read from librados object
	//io.Copy(out, obj)

}
