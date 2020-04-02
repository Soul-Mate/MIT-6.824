package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// reduce collects immediate files generated during the map phase

	// md is encode immediate file write to memory data
	md := make(map[string][]string)

	// the mi is map worker location on the cluster (abstraction cluster)
	for mi := 0; mi < nMap; mi++ {
		immedidateFile := reduceName(jobName, mi, reduceTaskNumber)

		fr, err := os.Open(immedidateFile)
		if err != nil {
			log.Printf("open %s immedidate file failed: %s", immedidateFile, err.Error())
			continue
		}

		// encode immediate file and write to memory data
		dec := json.NewDecoder(fr)
		for {
			kv := KeyValue{}
			if err == dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}

				log.Printf("json encode kv failed: %s", err)
				continue
			}

			// write
			md[kv.Key] = append(memoryData[kv.Key], kv.Value)
		}
		fr.Close()
		dec.Close()
	}

	// run reduce, write reduce output
	fw, err := os.Open(outFile)
	if err != nil {
		panic(err)
	}

	enc := json.NewEncoder(fw)
	for k, vs := range md {
		err = enc.Encode(KeyValue{k, reduceF(k, vs)})
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Printf("json encode kv failed: %s", err)
			continue
		}
	}

	fw.Close()
	enc.Close()
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
}
