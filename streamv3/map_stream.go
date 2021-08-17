package streamv3

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

// MapStream MapStream
type MapStream interface {
	Parallel(parallel int) MapStream
	// 根据filter func过滤符合条件的elem
	// filter参数应为 func (key K, val V) bool，K为map结构的key类型，V为map结构的value类型
	Filter(filter ...interface{}) MapStream
	// 根据mapper func将stream中的elem对象转化成另一种对象
	// mapper参数应为 func (item T) O，T为上游数据类型，O为产出的新数据类型
	Map(mapper interface{}) SliceStream
	// 根据mapper func将stream中的elem对象转化成另一种对象
	// mapper参数应为 func (item T) []O，T为上游数据类型，O为产出的新数据类型，并将[]O打平
	FlatMap(mapper interface{}) SliceStream
	// KeysToStream 获取keys SliceStream
	KeysToStream() SliceStream
	// ValuesToStream 获取values SliceStream
	ValuesToStream() SliceStream
}

// MapStreamer MapStreamer
// 在Streamer上链式惰性操作，会形成一个链表的结构（通过lastStreamer连接）
// 在这个链表上的每一个节点（除了头节点持有了data slice），都不持有具体的数据。
// 即不保存数据本身，而是保存操作。
type MapStreamer struct {
	lastStreamer *MapStreamer
	parallel     int
	filterFunc   []reflect.Value
	mapFunc      *reflect.Value
	flatMapFunc  *reflect.Value
	pairData     []pair
	curKeyType   reflect.Type
	curValueType reflect.Type
}

// OfMap 只接受map类型
func OfMap(data interface{}) MapStream {
	val := reflect.ValueOf(data)
	dt := reflect.TypeOf(data)
	kind := val.Kind()
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		dt = dt.Elem()
	}
	if val.Kind() != reflect.Map {
		panic(fmt.Errorf("mapIter must be map or map pointer, not %s", kind))
	}
	mapIter := val.MapRange()
	pairData := []pair{}
	for mapIter.Next() {
		pairData = append(pairData, pair{
			key:   mapIter.Key().Interface(),
			value: mapIter.Value().Interface(),
		})
	}
	s := &MapStreamer{
		lastStreamer: nil,
		parallel:     1,
		filterFunc:   nil,
		mapFunc:      nil,
		//mapIter:      val.MapRange(),
		pairData:     pairData,
		curKeyType:   val.Type().Key(),
		curValueType: val.Type().Elem(),
	}
	return s
}

// Parallel 设置并行度
func (streamer *MapStreamer) Parallel(parallel int) MapStream {
	// at least 1 parallel
	if parallel <= 0 {
		parallel = 1
	}
	// max parallel = 2 * cpu_num
	if parallel > runtime.NumCPU()*2 {
		parallel = runtime.NumCPU() * 2
	}
	streamer.parallel = parallel
	return streamer
}

// Filter 过滤规则，filter的参数elem是stream中的元素
// 若调用者在filter中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *MapStreamer) Filter(filters ...interface{}) MapStream {
	fvs := []reflect.Value{}
	for i := 0; i < len(filters); i++ {
		filter := filters[i]
		fv := reflect.ValueOf(filter)
		if fv.Kind() != reflect.Func {
			panic(fmt.Errorf("filter must be a function, not %s", fv.Kind()))
		}
		ft := fv.Type()
		if ft.NumIn() != 2 {
			panic(fmt.Errorf("filter's args number must equals 2, not %d", ft.NumIn()))
		}

		ip1 := ft.In(0)
		if streamer.curKeyType != ip1 {
			panic(fmt.Errorf("key's type is %s, but filter's key type is %s", streamer.curKeyType, ip1))
		}
		ip2 := ft.In(1)
		if streamer.curValueType != ip2 {
			panic(fmt.Errorf("key's type is %s, but filter's key type is %s", streamer.curValueType, ip2))
		}

		if ft.NumOut() != 1 {
			panic(fmt.Errorf("filter's output number must equals 1, not %d", ft.NumOut()))
		}
		op1 := ft.Out(0)
		if op1.Kind() != reflect.Bool {
			panic(fmt.Errorf("filter's return-val type should be bool, not %s", op1))
		}
		fvs = append(fvs, fv)
	}

	return &MapStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   fvs,
		mapFunc:      nil,
		curKeyType:   streamer.curKeyType,
		curValueType: streamer.curValueType,
	}
}

// Map 转化规则，mapper的参数elem是stream中的元素，mapper返回值则会继续进入stream
// 若调用者在mapper中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *MapStreamer) Map(mapper interface{}) SliceStream {
	fv := reflect.ValueOf(mapper)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("mapper must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 2 {
		panic(fmt.Errorf("mapper's args number must equals 2, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curKeyType != ip1 {
		panic(fmt.Errorf("key's type is %s, but mapper's key type is %s", streamer.curKeyType, ip1))
	}
	ip2 := ft.In(1)
	if streamer.curValueType != ip2 {
		panic(fmt.Errorf("key's type is %s, but filter's key type is %s", streamer.curValueType, ip2))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("mapper's output number must equals 1, not %d", ft.NumOut()))
	}

	newStreamer := &MapStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      &fv,
		flatMapFunc:  nil,
		curKeyType:   streamer.curKeyType,
		curValueType: streamer.curValueType,
	}

	return &SliceStreamer{
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		lastStreamer: nil,
		dataGetter: &mapGetter{
			steamer: newStreamer,
		},
		parallel:   streamer.parallel,
		filterFunc: nil,
		mapFunc:    nil,
		curType:    ft.Out(0),
	}
}

// FlatMap 转化规则，mapper的参数elem是stream中的元素，mapper返回值则会打平后继续进入stream
// 若调用者在mapper中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *MapStreamer) FlatMap(flatMapper interface{}) SliceStream {
	fv := reflect.ValueOf(flatMapper)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("flatMapper must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 2 {
		panic(fmt.Errorf("flatMapper's args number must equals 2, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curKeyType != ip1 {
		panic(fmt.Errorf("key's type is %s, but flatMapper's key type is %s", streamer.curKeyType, ip1))
	}
	ip2 := ft.In(1)
	if streamer.curValueType != ip2 {
		panic(fmt.Errorf("key's type is %s, but flatMapper's key type is %s", streamer.curValueType, ip2))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("flatMapper's output number must equals 1, not %d", ft.NumOut()))
	}

	op1 := ft.Out(0)
	if op1.Kind() != reflect.Slice {
		panic(fmt.Errorf("flatMapper's output must be slice"))
	}

	newStreamer := &MapStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		flatMapFunc:  &fv,
		curKeyType:   streamer.curKeyType,
		curValueType: streamer.curValueType,
	}

	return &SliceStreamer{
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		lastStreamer: nil,
		dataGetter: &mapGetter{
			steamer: newStreamer,
		},
		parallel:   streamer.parallel,
		filterFunc: nil,
		mapFunc:    nil,
		curType:    op1.Elem(),
	}
}

// KeysToStream 获取key的SliceStreamer
func (streamer *MapStreamer) KeysToStream() SliceStream {
	streamerList := []*MapStreamer{}
	lastStreamer := streamer
	for ; lastStreamer != nil; lastStreamer = lastStreamer.lastStreamer {
		streamerList = append(streamerList, lastStreamer)
	}
	newData := make([]pair, 0, len(streamerList[len(streamerList)-1].pairData))
	newData = append(newData, streamerList[len(streamerList)-1].pairData...)
	for i := len(streamerList) - 1; i >= 0; i-- {
		if streamerList[i].filterFunc != nil {
			newData = streamerList[i].filter(newData)
		}
	}
	data := []interface{}{}
	for i := 0; i < len(newData); i++ {
		data = append(data, newData[i].key)
	}

	return &SliceStreamer{
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		lastStreamer: nil,
		dataGetter: &sliceGetter{
			data: data,
		},
		parallel:   streamer.parallel,
		filterFunc: nil,
		mapFunc:    nil,
		curType:    streamer.curKeyType,
	}
}

// ValuesToStream 获取value的SliceStreamer
func (streamer *MapStreamer) ValuesToStream() SliceStream {
	streamerList := []*MapStreamer{}
	lastStreamer := streamer
	for ; lastStreamer != nil; lastStreamer = lastStreamer.lastStreamer {
		streamerList = append(streamerList, lastStreamer)
	}
	newData := make([]pair, 0, len(streamerList[len(streamerList)-1].pairData))
	newData = append(newData, streamerList[len(streamerList)-1].pairData...)
	for i := len(streamerList) - 1; i >= 0; i-- {
		if streamerList[i].filterFunc != nil {
			newData = streamerList[i].filter(newData)
		}
	}
	data := []interface{}{}
	for i := 0; i < len(newData); i++ {
		data = append(data, newData[i].value)
	}

	return &SliceStreamer{
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		lastStreamer: nil,
		dataGetter: &sliceGetter{
			data: data,
		},
		parallel:   streamer.parallel,
		filterFunc: nil,
		mapFunc:    nil,
		curType:    streamer.curValueType,
	}
}

/*
 * ============================================
 * 				inner implement
 * ============================================
 */

type pair struct {
	key   interface{}
	value interface{}
}

// scan 内部实现，用于其他方法复用
func (streamer *MapStreamer) scan() []interface{} {
	streamerList := []*MapStreamer{}
	lastStreamer := streamer
	for ; lastStreamer != nil; lastStreamer = lastStreamer.lastStreamer {
		streamerList = append(streamerList, lastStreamer)
	}
	newData := make([]pair, 0, len(streamerList[len(streamerList)-1].pairData))
	newData = append(newData, streamerList[len(streamerList)-1].pairData...)
	for i := len(streamerList) - 1; i >= 0; i-- {
		if streamerList[i].filterFunc != nil {
			newData = streamerList[i].filter(newData)
		}
		if streamerList[i].flatMapFunc != nil {
			return streamerList[i].flatMap(newData)
		}
		if streamerList[i].mapFunc != nil {
			return streamerList[i]._map(newData)
		}
	}
	return []interface{}{}
}

// filter 内部实现，用于其他方法复用
func (streamer *MapStreamer) filter(data []pair) (result []pair) {
	if len(streamer.filterFunc) == 0 {
		return data
	}
	var wg sync.WaitGroup
	var panicError error
	wg.Add(streamer.parallel)
	batch := len(data) / streamer.parallel
	results := make([][]pair, streamer.parallel, streamer.parallel)
	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(data) {
			end = len(data)
		}
		go func(goroutineID, start, end int) {
			defer func() {
				if r := recover(); r != nil {
					panicError = fmt.Errorf("panic: %s", r)
				}
				wg.Done()
			}()
			res := []pair{}
			for i := start; i < end; i++ {
				isFilter := true
				for j := 0; j < len(streamer.filterFunc); j++ {
					op := call(streamer.filterFunc[j], data[i].key, data[i].value)
					isFilter = op[0].Bool()
					if !isFilter {
						break
					}
				}
				if isFilter {
					res = append(res, data[i])
				}
			}
			results[goroutineID] = res
		}(i, start, end)
	}
	wg.Wait()
	// 内部多个goroutine并行，将内部panic放回主goroutine中
	if panicError != nil {
		panic(panicError)
	}
	for i := 0; i < len(results); i++ {
		result = append(result, results[i]...)
	}
	return result
}

// _map 内部实现，用于其他方法复用
func (streamer *MapStreamer) _map(data []pair) (result []interface{}) {
	if streamer.mapFunc == nil {
		return []interface{}{}
	}
	var wg sync.WaitGroup
	var panicError error
	wg.Add(streamer.parallel)
	batch := len(data) / streamer.parallel
	results := make([][]interface{}, streamer.parallel, streamer.parallel)
	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(data) {
			end = len(data)
		}
		go func(goroutineID, start, end int) {
			defer func() {
				if r := recover(); r != nil {
					panicError = fmt.Errorf("panic: %s", r)
				}
				wg.Done()
			}()
			res := []interface{}{}
			for i := start; i < end; i++ {
				op := call(*streamer.mapFunc, data[i].key, data[i].value)
				res = append(res, op[0].Interface())
			}
			results[goroutineID] = res
		}(i, start, end)
	}
	wg.Wait()
	// 内部多个goroutine并行，将内部panic放回主goroutine中
	if panicError != nil {
		panic(panicError)
	}
	for i := 0; i < len(results); i++ {
		result = append(result, results[i]...)
	}
	return result
}

// flatMap 内部实现，用于其他方法复用
func (streamer *MapStreamer) flatMap(data []pair) (result []interface{}) {
	if streamer.flatMapFunc == nil {
		return []interface{}{}
	}
	var wg sync.WaitGroup
	var panicError error
	wg.Add(streamer.parallel)
	batch := len(data) / streamer.parallel
	results := make([][]interface{}, streamer.parallel, streamer.parallel)
	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(data) {
			end = len(data)
		}
		go func(goroutineID, start, end int) {
			defer func() {
				if r := recover(); r != nil {
					panicError = fmt.Errorf("panic: %s", r)
				}
				wg.Done()
			}()
			res := []interface{}{}
			for i := start; i < end; i++ {
				op := call(*streamer.flatMapFunc, data[i].key, data[i].value)
				for i := 0; i < op[0].Len(); i++ {
					res = append(res, op[0].Index(i).Interface())
				}
			}
			results[goroutineID] = res
		}(i, start, end)
	}
	wg.Wait()
	// 内部多个goroutine并行，将内部panic放回主goroutine中
	if panicError != nil {
		panic(panicError)
	}
	for i := 0; i < len(results); i++ {
		result = append(result, results[i]...)
	}
	return result
}
