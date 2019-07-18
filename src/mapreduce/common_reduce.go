package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

type ByKey []KeyValue

func (k ByKey) Len() int {
	return len(k)
}
func (k ByKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
func (k ByKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

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
	// objects to the file named outFile. We requireKeyValue you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:reduceName
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	outF, err := os.Create(outFile)
	defer outF.Close()
	if err != nil {
		log.Fatal("creating outfile", err)
	}
	enc := json.NewEncoder(outF)
	// var kvs []KeyValue
	kvs := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		fd, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
		defer fd.Close()

		if err != nil {
			log.Fatal("reduce reading file: ", err)
		}
		dec := json.NewDecoder(fd)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
			// kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}

	}

	sort.Sort(ByKey(kvs))

	if len(kvs) == 0 {
		return
	}

	var vals []string
	for i := 0; i < len(kvs); i++ {
		vals = append(vals, kvs[i].Value)
		if (i == len(kvs)-1) || (kvs[i].Key != kvs[i+1].Key) {
			enc.Encode(KeyValue{kvs[i].Key, reduceF(kvs[i].Key, vals)})
			vals = make([]string, 0)
		}
	}
}
