package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	//1. reduce是第r个reduce节点读取map节点中的第r个中间节点
	//   并且对数据进行定义好的reduce操作
	// key: key值　value: 一个字符串数组

	keyValueArray := make(map[string][]string)

	//用来记录所有的Key
	keyArray := make([]string,0)

	for i:=0;i<nMap;i++{
		//从所有map节点中读取属于自己的中间文件。
		file,_ := os.Open(reduceName(jobName, i, reduceTask))
		defer file.Close()
		decoder:=json.NewDecoder(file)
		for{
			var kv KeyValue
			//解码,解码的内容的key与value放到KeyValue
			err:=decoder.Decode(&kv)
			if err != nil{
				break
			}
			_,ok := keyValueArray[kv.Key]
			if !ok {
				keyValueArray[kv.Key] = make([]string, 0)
				keyArray = append(keyArray, kv.Key)
			}
			keyValueArray[kv.Key] = append(keyValueArray[kv.Key], kv.Value)
		}
	}
	sort.Strings(keyArray)
	mergeSlice :=make([]KeyValue, 0)

	for _,key:=range keyArray{
		res:=reduceF(key, keyValueArray[key])
		mergeSlice=append(mergeSlice, KeyValue{key, res})
	}

	outputFile,_:=os.Create(outFile)
	defer outputFile.Close()
	encoder:=json.NewEncoder(outputFile)
	for _,kv:=range mergeSlice{
		//encode 是写入文件
		_ = encoder.Encode(&kv)
	}

}
