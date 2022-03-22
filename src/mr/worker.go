package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallForTask(mapf, reducef)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func CallForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 大循环，不断向服务器发送请求

		// 1秒请求一次
		time.Sleep(1000 * time.Millisecond)

		// 注意只能空定义！！！
		args := AskForTask_Args{}
		reply := AskForMapTask_Reply{}

		ok := call("Coordinator.AllocTask", &args, &reply)
		if ok {
		} else {
			fmt.Printf("Call failed!\n")
			return
		}

		if reply.AllFinished == true {
			// 所有任务都已完成，跳出循环
			break
		}

		if reply.TaskType == 3 {
			// 说明没有任务
			continue
		}

		if reply.TaskType == 1 {
			// 说明是map任务
			//mapf, _ := loadPlugin(os.Args[1])

			fmt.Println("File order: ", reply.FileOrder)
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			// 统计该篇文章的词频
			kva := mapf(reply.File, string(content))
			// 给单词排排序
			sort.Sort(ByKey(kva))

			// 将kva按照哈希值分组
			// 存临时文件的切片
			filehandlers := make([]*os.File, reply.NumReduce)

			for i := 0; i < reply.NumReduce; i++ {
				_, err := os.Stat("mr-tmp")
				if err != nil {
					if os.IsNotExist(err) {
						// 不存在则创建一个目录
						os.Mkdir("mr-tmp", os.ModePerm)
					}
				}
				tmpfile, err := ioutil.TempFile("mr-tmp", "mr-map-*")
				//tmpfile, err := ioutil.TempFile("./mr-tmp/",
				//	"mr-"+strconv.Itoa(reply.FileOrder)+"-"+strconv.Itoa(i))
				if err != nil {
					fmt.Printf("TempFile error!\n")
					return
				}
				filehandlers[i] = tmpfile

				// 防止异常返回
				defer func(tmpfile *os.File) {
					err := tmpfile.Close()
					if err != nil {
						fmt.Printf("Close file error!\n")
					}
				}(tmpfile)
			}

			lastorder := ihash(kva[0].Key) % reply.NumReduce
			// 先打开第一个文件
			enc := json.NewEncoder(filehandlers[0])
			err = enc.Encode(&kva[0])
			if err != nil {
				fmt.Printf("Encode error!\n")
				return
			}
			for i := 1; i < len(kva); i++ {
				// 拿出单词
				order := ihash(kva[i].Key) % reply.NumReduce

				if order == lastorder {
					// 说明是同一个文件
					// 加到同一个文件里
					err := enc.Encode(&kva[i])
					if err != nil {
						fmt.Printf("Encode error!\n")
						return
					}
				} else {
					// 不是同一个文件就再开一个新的
					//fmt.Println("Not the same key.")
					enc = json.NewEncoder(filehandlers[order])
					err := enc.Encode(&kva[i])
					if err != nil {
						fmt.Printf("Encode error!\n")
						return
					}
				}
				lastorder = order
			}

			//fmt.Println("FileOrder: ", reply.FileOrder)

			for i, tmpfile := range filehandlers {
				// 把文件改为正确的名字
				_, err := os.Stat("mr-inter")
				if err != nil {
					if os.IsNotExist(err) {
						// 不存在则创建一个目录
						os.Mkdir("mr-inter", os.ModePerm)
					}
				}
				//fmt.Printf("%s", tmpfile.Name())
				err = os.Rename(tmpfile.Name(), "mr-inter/mr-"+strconv.Itoa(reply.FileOrder)+"-"+strconv.Itoa(i))
				//err := os.Rename(tmpfile.Name(), "mr-"+strconv.Itoa(reply.FileOrder)+"-"+strconv.Itoa(i))
				if err != nil {
					fmt.Printf("Rename error!\n")
					return
				}
			}

			fargs := InformFinished_Args{reply.File, 0}
			freply := InformFinished_Reply{}
			ok = call("Coordinator.ReceiveFinished", &fargs, &freply)

			continue
		}

		// 说明是reduce任务
		_, err := os.Stat("mr-tmp")
		if err != nil {
			if os.IsNotExist(err) {
				// 不存在则创建一个目录
				os.Mkdir("mr-tmp", os.ModePerm)
			}
		}
		tmpfile, err := ioutil.TempFile("mr-tmp", "mr-red-*")
		if err != nil {
			fmt.Printf("Tempfile error!\n")
			return
		}
		kva := []KeyValue{}
		// 读取所有的相同reduce序号的文件
		for i := 0; i < reply.NumMap; i++ {

			file, err := os.Open("mr-inter/mr-" + strconv.Itoa(i) +
				"-" + strconv.Itoa(reply.ReduceOrder))
			//file, err := os.Open("mr-" + strconv.Itoa(i) +
			//	"-" + strconv.Itoa(reply.ReduceOrder))
			//fmt.Println("ReduceOrder: ", reply.ReduceOrder)
			if err != nil {
				fmt.Printf("Open inter file error!\n")
				return
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		// 排序保证相同单词堆在一起
		sort.Sort(ByKey(kva))
		i := 0
		for i < len(kva) {
			j := i + 1
			// 因为单词一样的键值对是连续排的
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
			i = j
		}

		err = os.Rename(tmpfile.Name(), "mr-out-"+strconv.Itoa(reply.ReduceOrder))
		if err != nil {
			fmt.Printf("Rename error!\n")
			return
		}
		fargs := InformFinished_Args{"", reply.ReduceOrder}
		freply := InformFinished_Reply{}
		ok = call("Coordinator.ReceiveFinishedForReduce", &fargs, &freply)
		continue
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

////
//// load the application Map and Reduce functions
//// from a plugin file, e.g. ../mrapps/wc.so
////
//func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
