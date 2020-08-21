package stream

import (
	"errors"
	"reflect"
	"runtime"
	"sort"
	"sync"
)

// Stream Stream
type Stream interface {
	/*
	 * 惰性操作，不会立刻执行。只保存操作，不修改数据。
	 */

	// 设置并行度。并行度不是全局的概念，而是一个操作上的概念。
	// 比如并行度为k，数据量为n，那么在执行filter/map等操作时，会创建k个goroutine
	// 让每个goroutine上承担 n/k 的数据量（无法整除则将剩余数据并入最后一个goroutine）
	// streamer默认继承上一个streamer的并行度，如果没有上一个streamer，那么默认并行度为1。
	// 在某个操作上设置新的并行度，不会影响之前的操作的并行度。
	// 例如：
	// 		源数据较多，执行filter时可以设置较大的并行度，从而提高效率；
	// 		经过filter后的数据量已经不多了，那么可以在map上设置较小的并行度；
	// 		从而避免创建过多goroutine。
	// 上面说到并行度不是全局的概念，但可以通过某些操作实现全局的并行度设置。
	// 即可以在最初的streamer上设置全局并行度k，随后不再设置并行度，从而实现全局并行度k。
	Parallel(parallel int) Stream
	// 根据filter func过滤符合条件的elem
	Filter(filter func(elem interface{}) bool) Stream
	// 根据mapper func将stream中的elem对象转化成另一种对象
	Map(mapper func(elem interface{}) interface{}) Stream
	// 跳过前n条记录
	Offset(n int) Stream
	// 取前n条记录
	Limit(n int) Stream
	// 根据sorter的排序规则进行排序，sorter的结果为true则为降序，为false为升序
	Sorted(sorter func(elem1, elem2 interface{}) bool) Stream

	/*
	 * 终结操作，例如求值，会立刻执行。并且会执行累加的惰性操作。
	 */

	// 遍历所有结果，对每个结果执行希望的op func
	Foreach(op func(elem interface{}) error) error
	// 将结果读取出来，调用者根据stream中的元素类型，传入相应的slice pointer
	Scan(result interface{}) error
	// 根据getKey func获取key，并做聚合。聚合结果由result带出。
	GroupBy(getKey func(elem interface{}) interface{}, result interface{}) error
	// 获取结果中的第一个
	First(result interface{}) (bool, error)
	// 获取结果中的最后一个
	Last(result interface{}) (bool, error)
	// 获取结果中的第index个（从0开始计数）
	IndexAt(index int, result interface{}) (bool, error)
	// 获取元素数
	Count() int
}

// Streamer Streamer
// 在Streamer上链式惰性操作，会形成一个链表的结构（通过lastStreamer连接）
// 在这个链表上的每一个节点（除了头节点持有了data slice），都不持有具体的数据。
// 即不保存数据本身，而是保存操作。
type Streamer struct {
	lastStreamer *Streamer
	parallel     int
	filterFunc   func(elem interface{}) bool
	mapFunc      func(elem interface{}) interface{}
	sortFunc     func(first, second interface{}) bool
	offset       int
	limit        int
	data         []interface{}
}

// Filter 过滤规则，filter的参数elem是stream中的元素
// 若调用者在filter中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *Streamer) Filter(filter func(elem interface{}) bool) *Streamer {
	return &Streamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   filter,
		mapFunc:      nil,
		sortFunc:     nil,
		offset:       streamer.offset,
		limit:        streamer.limit,
	}
}

// Map 转化规则，mapper的参数elem是stream中的元素，mapper返回值则会继续进入stream
// 若调用者在mapper中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *Streamer) Map(mapper func(elem interface{}) interface{}) *Streamer {
	return &Streamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      mapper,
		sortFunc:     nil,
		offset:       streamer.offset,
		limit:        streamer.limit,
	}
}

// Foreach 遍历streamer中的每个元素
func (streamer *Streamer) Foreach(op func(elem interface{}) error) error {
	result, err := streamer.scan()
	if err != nil {
		return err
	}
	for i := 0; i < len(result); i++ {
		err = op(result[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Parallel 设置并行度
func (streamer *Streamer) Parallel(parallel int) *Streamer {
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

// Scan 将结果带出
func (streamer *Streamer) Scan(result interface{}) error {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return errors.New("result must be slice pointer")
	}
	val = val.Elem()
	// nil map init
	if val.IsNil() {
		val.Set(reflect.MakeSlice(val.Type(), 0, 0))
	}
	scanResult, err := streamer.scan()
	if err != nil {
		return err
	}
	// 先清空已有数据
	val.SetLen(0)
	for i := 0; i < len(scanResult); i++ {
		val.Set(reflect.Append(val, reflect.ValueOf(scanResult[i])))
	}
	return nil
}

// scan 内部实现，用于其他方法复用
func (streamer *Streamer) scan() ([]interface{}, error) {
	streamerList := []*Streamer{}
	lastStreamer := streamer
	for ; lastStreamer != nil; lastStreamer = lastStreamer.lastStreamer {
		streamerList = append(streamerList, lastStreamer)
	}
	data := streamerList[len(streamerList)-1].data
	for i := len(streamerList) - 1; i >= 0; i-- {
		if streamerList[i].filterFunc != nil {
			data = streamerList[i].filter(data)
		}
		if streamerList[i].mapFunc != nil {
			data = streamerList[i]._map(data)
		}
		if streamerList[i].sortFunc != nil {
			sort.Slice(data, func(first, second int) bool {
				return streamerList[i].sortFunc(data[first], data[second])
			})
		}
	}
	// offset limit
	offset := 0
	if streamer.offset < len(data) {
		offset = streamer.offset
	}
	limit := len(data) - offset
	if streamer.limit > 0 && streamer.limit < limit {
		limit = streamer.limit
	}
	data = data[offset : offset+limit]
	return data, nil
}

// filter 内部实现，用于其他方法复用
func (streamer *Streamer) filter(data []interface{}) (result []interface{}) {
	var wg sync.WaitGroup
	wg.Add(streamer.parallel)
	batch := len(data) / streamer.parallel
	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(data) {
			end = len(data)
		}
		go func(start, end int) {
			defer func() {
				wg.Done()
			}()
			for i := start; i < end; i++ {
				if streamer.filterFunc(data[i]) {
					result = append(result, data[i])
				}
			}
		}(start, end)
	}
	wg.Wait()
	return result
}

// _map 内部实现，用于其他方法复用
func (streamer *Streamer) _map(data []interface{}) (result []interface{}) {
	var wg sync.WaitGroup
	wg.Add(streamer.parallel)
	batch := len(data) / streamer.parallel
	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(data) {
			end = len(data)
		}
		go func(start, end int) {
			defer func() {
				wg.Done()
			}()
			for i := start; i < end; i++ {
				result = append(result, streamer.mapFunc(data[i]))
			}
		}(start, end)
	}
	wg.Wait()
	return result
}

// Limit 取前n条记录，惰性操作，只在执行了终结操作时起作用
func (streamer *Streamer) Limit(n int) *Streamer {
	if n <= 0 {
		panic("limit rows can't less than or equal 0")
	}
	return &Streamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		limit:        n,
		offset:       streamer.offset,
	}
}

// Offset 跳过前n条记录，惰性操作，只在执行了终结操作时起作用
func (streamer *Streamer) Offset(n int) *Streamer {
	if n <= 0 {
		panic("offset rows can't less than or equal 0")
	}
	return &Streamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		limit:        streamer.limit,
		offset:       n,
	}
}

// Sorted 排序
func (streamer *Streamer) Sorted(sorter func(elem1, elem2 interface{}) bool) *Streamer {
	return &Streamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		limit:        streamer.limit,
		offset:       streamer.offset,
		sortFunc:     sorter,
	}
}

// Count 计数
func (streamer *Streamer) Count() (int, error) {
	result, err := streamer.scan()
	if err != nil {
		return 0, err
	}
	return len(result), nil
}

// GroupBy 根据getKey函数获取key，并将group by结果作为一个result map带回
func (streamer *Streamer) GroupBy(getKey func(elem interface{}) interface{}, result interface{}) error {
	if getKey == nil {
		return errors.New("getKey func can't be nil")
	}
	val := reflect.ValueOf(result)
	kind := val.Kind()
	if kind == reflect.Ptr {
		if val.Elem().Kind() != reflect.Map {
			return errors.New("result must be map or map pointer")
		}
		val = val.Elem()
		// nil map init
		if val.IsNil() {
			val.Set(reflect.MakeMap(val.Type()))
		}
	}
	if val.Kind() != reflect.Map {
		return errors.New("result must be map or map pointer")
	}
	scanResult, err := streamer.scan()
	if err != nil {
		return err
	}
	for i := 0; i < len(scanResult); i++ {
		key := getKey(scanResult[i])
		value := val.MapIndex(reflect.ValueOf(key))
		if !value.IsValid() {
			value = reflect.MakeSlice(reflect.SliceOf(reflect.ValueOf(scanResult[i]).Type()), 0, 0)
		}
		value = reflect.Append(value, reflect.ValueOf(scanResult[i]))
		val.SetMapIndex(reflect.ValueOf(key), value)
	}
	return nil
}

// First 取第一个结果
func (streamer *Streamer) First(result interface{}) (exist bool, err error) {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		return false, errors.New("result must be a pointer")
	}
	scanResult, err := streamer.scan()
	if err != nil {
		return false, err
	}
	return streamer.indexAt(0, scanResult, result)
}

// Last 取最后一个结果
func (streamer *Streamer) Last(result interface{}) (bool, error) {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		return false, errors.New("result must be a pointer")
	}
	scanResult, err := streamer.scan()
	if err != nil {
		return false, err
	}
	return streamer.indexAt(len(scanResult)-1, scanResult, result)
}

// IndexAt 取第index个结果（从0开始计数）
func (streamer *Streamer) IndexAt(index int, result interface{}) (bool, error) {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		return false, errors.New("result must be a pointer")
	}
	scanResult, err := streamer.scan()
	if err != nil {
		return false, err
	}
	return streamer.indexAt(index, scanResult, result)
}

// indexAt IndexAt的内部实现
func (streamer *Streamer) indexAt(index int, scanResult []interface{}, result interface{}) (bool, error) {
	val := reflect.ValueOf(result).Elem()
	if len(scanResult) <= index {
		return false, nil
	}
	val.Set(reflect.ValueOf(scanResult[index]))
	return true, nil
}

// NewStreamerWithData 只接受slice类型
func NewStreamerWithData(data interface{}) (*Streamer, error) {
	interfaceList := []interface{}{}
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		if val.Elem().Kind() != reflect.Slice {
			return nil, errors.New("data must be slice or slice pointer")
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Slice {
		return nil, errors.New("data must be slice or slice pointer")
	}
	for i := 0; i < val.Len(); i++ {
		interfaceList = append(interfaceList, val.Index(i).Interface())
	}
	return &Streamer{
		lastStreamer: nil,
		parallel:     1,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		data:         interfaceList,
	}, nil
}
