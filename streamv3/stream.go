package streamv3

type DataGetter interface {
	getData() []interface{}
}

type sliceGetter struct {
	data []interface{}
}

func (getter *sliceGetter) getData() []interface{} {
	return getter.data
}

type mapGetter struct {
	steamer *MapStreamer
}

func (getter *mapGetter) getData() []interface{} {
	return getter.steamer.scan()
}
