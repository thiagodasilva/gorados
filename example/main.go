package main

import (
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

	/*out, e := os.Create("/home/vagrant/output.txt")
	if e != nil {
		panic(e)
	}*/

	// read from librados object
	//io.Copy(out, obj)

}
