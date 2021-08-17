package streamv3

import (
	"strings"
	"testing"
)

var testDataMap = map[int64]testUser{
	1:{
		ID:    1,
		Name:  "zhangsan",
		Age:   15,
		Email: "zhangsan@xxx.com",
	},
	2:{
		ID:    2,
		Name:  "lisi",
		Age:   15,
		Email: "lisi@xxx.com",
	},
	3:{
		ID:    3,
		Name:  "wangwu",
		Age:   20,
		Email: "wangwu@xxx.com",
	},
	4:{
		ID:    4,
		Name:  "zhaoliu",
		Age:   25,
		Email: "zhaoliu@xxx.com",
	},
}

var mapStreamer MapStream

func init() {
	mapStreamer = OfMap(testDataMap)
}

func TestMapStreamerFilter(t *testing.T) {
	result := []testUser{}
	mapStreamer.Filter(func(key int64, val testUser) bool {
		return key % 2 == 0
	}).Map(func(key int64, val testUser) testUser {
		return val
	}).Scan(&result)

	expectedResult := []testUser{
		{
			ID:    2,
			Name:  "lisi",
			Age:   15,
			Email: "lisi@xxx.com",
		},
		{
			ID:    4,
			Name:  "zhaoliu",
			Age:   25,
			Email: "zhaoliu@xxx.com",
		},
	}
	assertEquals(t, result, expectedResult)
}

func TestMapStreamerFlatMap(t *testing.T) {
	result := []string{}
	mapStreamer.FlatMap(func(key int64, val testUser) []string {
		return strings.Split(val.Email, "@")
	}).Scan(&result)

	expectedResult := []string{
		"zhangsan", "xxx.com", "lisi", "xxx.com", "wangwu", "xxx.com", "zhaoliu", "xxx.com",
	}
	assertEquals(t, result, expectedResult)
}

func TestMapStreamerMap(t *testing.T) {
	result := []int64{}
	mapStreamer.Map(func(key int64, val testUser) int64 {
		return key
	}).Scan(&result)

	expectedResult := []int64{1,2,3,4}
	assertEquals(t, result, expectedResult)
}

func TestMapStreamerKeysToStream(t *testing.T) {
	result := []int64{}
	mapStreamer.KeysToStream().Sorted(func (id1, id2 int64) bool{
		return id1 < id2
	}).Scan(&result)

	expectedResult := []int64{1,2,3,4}
	assertEquals(t, result, expectedResult)
}

func TestMapStreamerValuesToStream(t *testing.T) {
	result := []testUser{}
	mapStreamer.ValuesToStream().Sorted(func (item1, item2 testUser) bool{
		return item1.ID < item2.ID
	}).Scan(&result)

	expectedResult := []testUser{
		{
			ID:    1,
			Name:  "zhangsan",
			Age:   15,
			Email: "zhangsan@xxx.com",
		},
		{
			ID:    2,
			Name:  "lisi",
			Age:   15,
			Email: "lisi@xxx.com",
		},
		{
			ID:    3,
			Name:  "wangwu",
			Age:   20,
			Email: "wangwu@xxx.com",
		},
		{
			ID:    4,
			Name:  "zhaoliu",
			Age:   25,
			Email: "zhaoliu@xxx.com",
		},
	}
	assertEquals(t, result, expectedResult)
}
