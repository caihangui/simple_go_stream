package streamv3

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// SliceStream SliceStream
type SliceStream interface {
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
	Parallel(parallel int) SliceStream
	// 根据filter func过滤符合条件的elem
	// filter参数应为 func (item T) bool，T为上游数据类型
	Filter(filter ...interface{}) SliceStream
	// 根据mapper func将stream中的elem对象转化成另一种对象
	// mapper参数应为 func (item T) O，T为上游数据类型，O为产出的新数据类型
	Map(mapper interface{}) SliceStream
	// 根据mapper func将stream中的elem对象转化成另一种对象
	// mapper参数应为 func (item T) []O，T为上游数据类型，O为产出的新数据类型，并将[]O打平
	FlatMap(mapper interface{}) SliceStream
	// 跳过前n条记录
	Offset(n int) SliceStream
	// 取前n条记录
	Limit(n int) SliceStream
	// 根据sorter的排序规则进行排序，sorter的结果为true则为降序，为false为升序
	// sorter参数应为 func (item1, item2 T) bool，T为上游数据类型
	Sorted(sorter interface{}) SliceStream

	/*
	 * 终结操作，例如求值，会立刻执行。并且会执行累加的惰性操作。
	 */

	// 遍历所有结果，对每个结果执行希望的op func
	// foreachOp参数应为 func (item T)，T为上游数据类型
	Foreach(foreachOps ...interface{})
	// 将结果读取出来，调用者根据stream中的元素类型，传入相应的slice pointer
	// result参数应为 []T类型，T为上游数据类型
	Scan(result interface{})
	// 根据getKey func获取key，并做聚合。聚合结果由result带出。
	// keyer参数应为 func (item T) K ，T为上游数据类型，K为 groupby key的类型
	// result参数应为map[K][]T
	GroupBy(keyer interface{}, result interface{})
	// 根据getKey func获取key，结果由result带出。
	// ToMap和GroupBy的区别是，ToMap需要调用者保证key的唯一性，若数据中key重复，会直接覆盖
	// keyer参数应为 func (item T) K ，T为上游数据类型，K为 tomap key的类型
	// result参数应为map[K]T
	ToMap(keyer interface{}, result interface{})
	// 获取结果中的第一个
	// result参数应为T类型，T为上游数据类型
	First(result interface{}) bool
	// 获取结果中的最后一个
	// result参数应为T类型，T为上游数据类型
	Last(result interface{}) bool
	// 获取结果中的第index个（从0开始计数）
	// result参数应为T类型，T为上游数据类型
	IndexAt(index int, result interface{}) bool
	// 获取元素数
	Count() int
	// 根据accumulator两两聚合，结果由result带出。
	// accumulator参数应为 func (item1, item2 T) T ，T为上游数据类型
	// result参数应为T类型
	Reduce(accumulator interface{}, result interface{})
}

// SliceStreamer SliceStreamer
// 在Streamer上链式惰性操作，会形成一个链表的结构（通过lastStreamer连接）
// 在这个链表上的每一个节点（除了头节点持有了data slice），都不持有具体的数据。
// 即不保存数据本身，而是保存操作。
type SliceStreamer struct {
	lastStreamer *SliceStreamer
	dataGetter   DataGetter
	parallel     int
	filterFunc   []reflect.Value
	mapFunc      *reflect.Value
	flatMapFunc  *reflect.Value
	sortFunc     *reflect.Value
	offset       int
	limit        int
	//data         []interface{}
	curType      reflect.Type
}

// OfSlice 只接受slice类型
func OfSlice(data interface{}) SliceStream {
	interfaceList := []interface{}{}
	val := reflect.ValueOf(data)
	dt := reflect.TypeOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		dt = dt.Elem()
	}
	if val.Kind() != reflect.Slice {
		panic(fmt.Errorf("mapIter must be slice pointer, not %s", val.Elem().Kind()))
	}
	s := &SliceStreamer{
		lastStreamer: nil,
		parallel:     1,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		offset:       0,
		limit:        0,
		//data:         interfaceList,
	}
	s.curType = dt.Elem()
	for i := 0; i < val.Len(); i++ {
		interfaceList = append(interfaceList, val.Index(i).Interface())
	}
	s.dataGetter = &sliceGetter{
		data: interfaceList,
	}
	return s
}

// Parallel 设置并行度
func (streamer *SliceStreamer) Parallel(parallel int) SliceStream {
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
func (streamer *SliceStreamer) Filter(filters ...interface{}) SliceStream {
	fvs := []reflect.Value{}
	for i := 0; i < len(filters); i++ {
		filter := filters[i]
		fv := reflect.ValueOf(filter)
		if fv.Kind() != reflect.Func {
			panic(fmt.Errorf("filter must be a function, not %s", fv.Kind()))
		}
		ft := fv.Type()
		if ft.NumIn() != 1 {
			panic(fmt.Errorf("filter's args number must equals 1, not %d", ft.NumIn()))
		}

		ip1 := ft.In(0)
		if streamer.curType != ip1 {
			panic(fmt.Errorf("upstream mapIter's type is %s, but filter's args type is %s", streamer.curType, ip1))
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

	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   fvs,
		mapFunc:      nil,
		sortFunc:     nil,
		offset:       streamer.offset,
		limit:        streamer.limit,
		curType:      streamer.curType,
	}
}

// Map 转化规则，mapper的参数elem是stream中的元素，mapper返回值则会继续进入stream
// 若调用者在mapper中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *SliceStreamer) Map(mapper interface{}) SliceStream {
	fv := reflect.ValueOf(mapper)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("mapper must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 1 {
		panic(fmt.Errorf("mapper's args number must equals 1, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curType != ip1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but mapper's args type is %s", streamer.curType, ip1))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("mapper's output number must equals 1, not %d", ft.NumOut()))
	}
	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      &fv,
		sortFunc:     nil,
		offset:       streamer.offset,
		limit:        streamer.limit,
		curType:      ft.Out(0),
	}
}

// FlatMap 转化规则，mapper的参数elem是stream中的元素，mapper返回值则会打平后继续进入stream
// 若调用者在mapper中进行转型断言，需要调用者自己保证stream中的元素可以被转型断言
func (streamer *SliceStreamer) FlatMap(flatMapper interface{}) SliceStream {
	fv := reflect.ValueOf(flatMapper)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("flatMapper must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 1 {
		panic(fmt.Errorf("flatMapper's args number must equals 1, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curType != ip1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but flatMapper's args type is %s", streamer.curType, ip1))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("flatMapper's output number must equals 1, not %d", ft.NumOut()))
	}
	op1 := ft.Out(0)
	if op1.Kind() != reflect.Slice {
		panic(fmt.Errorf("flatMapper's output must be slice"))
	}
	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		flatMapFunc:  &fv,
		sortFunc:     nil,
		offset:       streamer.offset,
		limit:        streamer.limit,
		curType:      op1.Elem(),
	}
}

// Limit 取前n条记录，惰性操作，只在执行了终结操作时起作用
func (streamer *SliceStreamer) Limit(n int) SliceStream {
	if n <= 0 {
		panic(fmt.Errorf("limit rows can't less than or equal 0, but your args is %d", n))
	}
	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		limit:        n,
		offset:       streamer.offset,
		curType:      streamer.curType,
	}
}

// Offset 跳过前n条记录，惰性操作，只在执行了终结操作时起作用
func (streamer *SliceStreamer) Offset(n int) SliceStream {
	if n <= 0 {
		panic(fmt.Errorf("offset rows can't less than or equal 0, but your args is %d", n))
	}
	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		sortFunc:     nil,
		limit:        streamer.limit,
		offset:       n,
		curType:      streamer.curType,
	}
}

// Sorted 排序
func (streamer *SliceStreamer) Sorted(sorter interface{}) SliceStream {
	fv := reflect.ValueOf(sorter)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("sorter must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 2 {
		panic(fmt.Errorf("sorter's args number must equals 2, not %d", ft.NumIn()))
	}
	ip1 := ft.In(0)
	ip2 := ft.In(1)
	if ip1 != ip2 {
		panic(fmt.Errorf("sorter: first param type (%s) is different with second param type (%s)", ip1, ip2))
	}

	if ip1 != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but sorter's args type is %s", streamer.curType, ip1))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("sorter's output number must equals 1, not %d", ft.NumOut()))
	}
	op1 := ft.Out(0)
	if op1.Kind() != reflect.Bool {
		panic(fmt.Errorf("sorter's return-val type should be bool, not %s", op1))
	}

	return &SliceStreamer{
		lastStreamer: streamer,
		parallel:     streamer.parallel,
		filterFunc:   nil,
		mapFunc:      nil,
		limit:        streamer.limit,
		offset:       streamer.offset,
		sortFunc:     &fv,
		curType:      streamer.curType,
	}
}

// Foreach 遍历streamer中的每个元素
func (streamer *SliceStreamer) Foreach(foreachOps ...interface{}) {
	fvs := []reflect.Value{}
	for i := 0; i < len(foreachOps); i++ {
		foreachOp := foreachOps[i]
		fv := reflect.ValueOf(foreachOp)
		if fv.Kind() != reflect.Func {
			panic(fmt.Errorf("foreachOp must be a function, not %s", fv.Kind()))
		}
		ft := fv.Type()
		if ft.NumIn() != 1 {
			panic(fmt.Errorf("foreachOp's args number must equals 1, not %d", ft.NumIn()))
		}

		ip1 := ft.In(0)
		if streamer.curType != ip1 {
			panic(fmt.Errorf("upstream mapIter's type is %s, but foreachOp's args type is %s", streamer.curType, ip1))
		}

		if ft.NumOut() != 0 {
			panic(fmt.Errorf("foreachOp's output number must equals 0, not %d", ft.NumOut()))
		}
		fvs = append(fvs, fv)
	}

	result := streamer.scan()
	for i := 0; i < len(result); i++ {
		for j := 0; j < len(fvs); j++ {
			_ = call(fvs[j], result[i])
		}
	}
}

// Scan 将结果带出
func (streamer *SliceStreamer) Scan(result interface{}) {
	val := reflect.ValueOf(result)
	rt := reflect.TypeOf(result)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		panic(errors.New("result must be slice pointer"))
	}
	val = val.Elem()
	rt = rt.Elem().Elem()
	if rt != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but Scan's args type is %s", streamer.curType, rt))
	}
	// nil map init
	if val.IsNil() {
		val.Set(reflect.MakeSlice(val.Type(), 0, 0))
	}
	scanResult := streamer.scan()
	// 先清空已有数据
	val.SetLen(0)
	for i := 0; i < len(scanResult); i++ {
		val.Set(reflect.Append(val, reflect.ValueOf(scanResult[i])))
	}
}

// Count 计数
func (streamer *SliceStreamer) Count() int {
	result := streamer.scan()
	return len(result)
}

// GroupBy 根据getKey函数获取key，并将group by结果作为一个result map带回
func (streamer *SliceStreamer) GroupBy(keyer interface{}, result interface{}) {
	if keyer == nil {
		panic(errors.New("keyer func can't be nil"))
	}
	fv := reflect.ValueOf(keyer)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("keyer must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 1 {
		panic(fmt.Errorf("keyer's args number must equals 1, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curType != ip1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but keyer's args type is %s", streamer.curType, ip1))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("keyer's output number must equals 1, not %d", ft.NumOut()))
	}
	op1 := ft.Out(0)
	val := reflect.ValueOf(result)
	rt := reflect.TypeOf(result)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		rt = rt.Elem()
	}
	if val.Kind() != reflect.Map {
		panic(fmt.Errorf("GroupBy result must be map or map pointer, not %s", val.Kind()))
	}
	if rt.Key() != op1 {
		panic(fmt.Errorf("keyer's return-value type is %s, but GroupBy result's key type is %s", op1, rt.Key()))
	}
	if rt.Elem().Elem() != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but GroupBy result's value type is %s", streamer.curType, rt.Elem().Elem()))
	}
	// nil map init
	if val.IsNil() {
		val.Set(reflect.MakeMap(val.Type()))
	}

	scanResult := streamer.scan()
	streamer.groupBy(fv, scanResult, &val)
}

// ToMap 根据getKey函数获取key，并将to map结果作为一个result map带回
func (streamer *SliceStreamer) ToMap(keyer interface{}, result interface{}) {
	if keyer == nil {
		panic(errors.New("keyer func can't be nil"))
	}
	fv := reflect.ValueOf(keyer)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("keyer must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 1 {
		panic(fmt.Errorf("keyer's args number must equals 1, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curType != ip1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but keyer's args type is %s", streamer.curType, ip1))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("keyer's output number must equals 1, not %d", ft.NumOut()))
	}
	op1 := ft.Out(0)
	val := reflect.ValueOf(result)
	rt := reflect.TypeOf(result)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		rt = rt.Elem()
	}
	if val.Kind() != reflect.Map {
		panic(fmt.Errorf("GroupBy result must be map or map pointer, not %s", val.Kind()))
	}
	if rt.Key() != op1 {
		panic(fmt.Errorf("keyer's return-value type is %s, but GroupBy result's key type is %s", op1, rt.Key()))
	}
	if rt.Elem() != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but GroupBy result's value type is %s", streamer.curType, rt.Elem()))
	}
	// nil map init
	if val.IsNil() {
		val.Set(reflect.MakeMap(val.Type()))
	}

	scanResult := streamer.scan()
	streamer.toMap(fv, scanResult, &val)
}

// Reduce 根据accumulator两两聚合，结果由result带出
func (streamer *SliceStreamer) Reduce(accumulator interface{}, result interface{}) {
	fv := reflect.ValueOf(accumulator)
	if fv.Kind() != reflect.Func {
		panic(fmt.Errorf("accumulator must be a function, not %s", fv.Kind()))
	}
	ft := fv.Type()
	if ft.NumIn() != 2 {
		panic(fmt.Errorf("accumulator's args number must equals 2, not %d", ft.NumIn()))
	}

	ip1 := ft.In(0)
	if streamer.curType != ip1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but accumulator's first args type is %s", streamer.curType, ip1))
	}

	ip2 := ft.In(1)
	if streamer.curType != ip2 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but accumulator's second args type is %s", streamer.curType, ip2))
	}

	if ft.NumOut() != 1 {
		panic(fmt.Errorf("accumulator's output number must equals 1, not %d", ft.NumOut()))
	}

	op1 := ft.Out(0)
	if streamer.curType != op1 {
		panic(fmt.Errorf("upstream mapIter's type is %s, but accumulator's return-value type is %s", streamer.curType, op1))
	}

	iv := reflect.ValueOf(result)
	if iv.Kind() != reflect.Ptr {
		panic(fmt.Errorf("result must be a %s ptr", streamer.curType))
	}

	if iv.Elem().Type() != streamer.curType {
		panic(fmt.Errorf("accumulator must be a %s, not %s", streamer.curType, iv.Elem().Type()))
	}
	streamer.reduce(fv, iv.Elem())
}

// First 取第一个结果
func (streamer *SliceStreamer) First(result interface{}) bool {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("result must be a pointer, not %d", val.Kind()))
	}
	val = val.Elem()
	if val.Type() != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but First's args type is %s", streamer.curType, val.Type()))
	}
	scanResult := streamer.scan()
	return streamer.indexAt(0, scanResult, val)
}

// Last 取最后一个结果
func (streamer *SliceStreamer) Last(result interface{}) bool {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		panic(errors.New("result must be a pointer"))
	}
	val = val.Elem()
	if val.Type() != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but Last's args type is %s", streamer.curType, val.Type()))
	}
	scanResult := streamer.scan()
	return streamer.indexAt(len(scanResult)-1, scanResult, val)
}

// IndexAt 取第index个结果（从0开始计数）
func (streamer *SliceStreamer) IndexAt(index int, result interface{}) bool {
	val := reflect.ValueOf(result)
	if val.Kind() != reflect.Ptr {
		panic(errors.New("result must be a pointer"))
	}
	val = val.Elem()
	if val.Type() != streamer.curType {
		panic(fmt.Errorf("upstream mapIter's type is %s, but IndexAt's args type is %s", streamer.curType, val.Type()))
	}

	scanResult := streamer.scan()
	return streamer.indexAt(index, scanResult, val)
}

/*
 * ============================================
 * 				inner implement
 * ============================================
 */

// scan 内部实现，用于其他方法复用
func (streamer *SliceStreamer) scan() []interface{} {
	streamerList := []*SliceStreamer{}
	lastStreamer := streamer
	for ; lastStreamer != nil; lastStreamer = lastStreamer.lastStreamer {
		streamerList = append(streamerList, lastStreamer)
	}
	data := streamerList[len(streamerList)-1].dataGetter.getData()
	newData := []interface{}{}
	newData = append(newData, data...)
	for i := len(streamerList) - 1; i >= 0; i-- {
		if streamerList[i].filterFunc != nil {
			newData = streamerList[i].filter(newData)
		}
		if streamerList[i].flatMapFunc != nil {
			newData = streamerList[i].flatMap(newData)
		}
		if streamerList[i].mapFunc != nil {
			newData = streamerList[i]._map(newData)
		}
		if streamerList[i].sortFunc != nil {
			sort.Slice(newData, func(first, second int) bool {
				op := call(*streamerList[i].sortFunc, newData[first], newData[second])
				return op[0].Bool()
			})
		}
	}
	// offset limit
	offset := 0
	if streamer.offset < len(newData) {
		offset = streamer.offset
	}
	limit := len(newData) - offset
	if streamer.limit > 0 && streamer.limit < limit {
		limit = streamer.limit
	}
	newData = newData[offset : offset+limit]
	return newData
}

// filter 内部实现，用于其他方法复用
func (streamer *SliceStreamer) filter(data []interface{}) (result []interface{}) {
	if len(streamer.filterFunc) == 0 {
		return data
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
				isFilter := true
				for j := 0; j < len(streamer.filterFunc); j++ {
					op := call(streamer.filterFunc[j], data[i])
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
func (streamer *SliceStreamer) _map(data []interface{}) (result []interface{}) {
	if streamer.mapFunc == nil {
		return data
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
				op := call(*streamer.mapFunc, data[i])
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

// reduce 内部实现，用于其他方法复用
func (streamer *SliceStreamer) reduce(fv, iv reflect.Value) {
	data := streamer.scan()
	if len(data) == 0 {
		return
	}
	if len(data) == 1 {
		iv.Set(reflect.ValueOf(data[0]))
		return
	}
	baseVal := iv
	for i := 0; i < len(data); i++ {
		baseVal = fv.Call([]reflect.Value{baseVal, reflect.ValueOf(data[i])})[0]
	}
	iv.Set(baseVal)
}

// flatMap 内部实现，用于其他方法复用
func (streamer *SliceStreamer) flatMap(data []interface{}) (result []interface{}) {
	if streamer.flatMapFunc == nil {
		return streamer.dataGetter.getData()
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
				op := call(*streamer.flatMapFunc, data[i])
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

// groupBy GroupBy内部实现，支持并行
func (streamer *SliceStreamer) groupBy(keyer reflect.Value, scanResult []interface{}, valPointer *reflect.Value) {
	var wg sync.WaitGroup
	var panicError error
	wg.Add(streamer.parallel)
	val := *valPointer
	batch := len(scanResult) / streamer.parallel
	// collect results from different worker goroutine
	// make the cap equals streamer.parallel, and use iteration index as goroutineID to avoid concurrent problem
	resultCollection := make(map[int]map[interface{}][]interface{}, streamer.parallel)

	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(scanResult) {
			end = len(scanResult)
		}
		// new worker goroutine
		go func(goroutineID, start, end int) {
			defer func() {
				if r := recover(); r != nil {
					panicError = fmt.Errorf("panic: %s", r)
				}
				wg.Done()
			}()
			curGoroutineMap := map[interface{}][]interface{}{}
			resultCollection[goroutineID] = curGoroutineMap
			for j := start; j < end; j++ {
				op := call(keyer, scanResult[j])
				key := op[0].Interface()
				valList := curGoroutineMap[key]
				if valList == nil {
					valList = make([]interface{}, 0, 1)
				}
				valList = append(valList, scanResult[j])
				curGoroutineMap[key] = valList
			}
		}(i, start, end)
	}
	wg.Wait()
	// 内部多个goroutine并行，将内部panic放回主goroutine中
	if panicError != nil {
		panic(panicError)
	}
	// merge results from different worker goroutine
	for i := 0; i < streamer.parallel; i++ {
		goroutineMap := resultCollection[i]
		for k, v := range goroutineMap {
			valList := val.MapIndex(reflect.ValueOf(k))
			if !valList.IsValid() {
				valList = reflect.MakeSlice(val.Type().Elem(), 0, len(v))
			}
			for j := 0; j < len(v); j++ {
				valList = reflect.Append(valList, reflect.ValueOf(v[j]))
			}
			val.SetMapIndex(reflect.ValueOf(k), valList)
		}
	}
}

func (streamer *SliceStreamer) toMap(keyer reflect.Value, scanResult []interface{}, valPointer *reflect.Value) {
	var wg sync.WaitGroup
	var panicError error
	wg.Add(streamer.parallel)
	val := *valPointer
	batch := len(scanResult) / streamer.parallel
	// collect results from different worker goroutine
	// make the cap equals streamer.parallel, and use iteration index as goroutineID to avoid concurrent problem
	resultCollection := make(map[int]map[interface{}]interface{}, streamer.parallel)

	for i := 0; i < streamer.parallel; i++ {
		start := i * batch
		end := start + batch
		if i == streamer.parallel-1 && end < len(scanResult) {
			end = len(scanResult)
		}
		// new worker goroutine
		go func(goroutineID, start, end int) {
			defer func() {
				if r := recover(); r != nil {
					panicError = fmt.Errorf("panic: %s", r)
				}
				wg.Done()
			}()
			curGoroutineMap := map[interface{}]interface{}{}
			resultCollection[goroutineID] = curGoroutineMap
			for j := start; j < end; j++ {
				op := call(keyer, scanResult[j])
				key := op[0].Interface()
				curGoroutineMap[key] = scanResult[j]
			}
		}(i, start, end)
	}
	wg.Wait()
	// 内部多个goroutine并行，将内部panic放回主goroutine中
	if panicError != nil {
		panic(panicError)
	}
	// merge results from different worker goroutine
	for i := 0; i < streamer.parallel; i++ {
		goroutineMap := resultCollection[i]
		for k, v := range goroutineMap {
			val.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
		}
	}
}

// indexAt IndexAt的内部实现
func (streamer *SliceStreamer) indexAt(index int, scanResult []interface{}, val reflect.Value) bool {
	if len(scanResult) <= index {
		return false
	}
	val.Set(reflect.ValueOf(scanResult[index]))
	return true
}

func call(fv reflect.Value, args ...interface{}) []reflect.Value {
	in := []reflect.Value{}
	for i := 0; i < len(args); i++ {
		in = append(in, reflect.ValueOf(args[i]))
	}
	return fv.Call(in)
}
