package streamv3

import (
	"reflect"
	"strings"
	"testing"
)

type testUser struct {
	ID    int
	Name  string
	Age   int
	Email string
}

var testData = []testUser{
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

var streamer *Streamer

func init() {
	streamer = NewStreamerWithData(testData)
}

// 仅限用于test，实际使用是reflect.DeepEqual的性能不行
func assertEquals(t *testing.T, result, expectedResult interface{}) {
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected_result: %v , but return %v", expectedResult, result)
	}
}

func TestNewStreamerWithData(t *testing.T) {
	_ = NewStreamerWithData(testData)
}

func TestStreamerFilter(t *testing.T) {
	result := []testUser{}
	streamer.Filter(func(elem testUser) bool {
		return elem.Age >= 18
	}).Scan(&result)
	expectedResult := []testUser{
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

	// multi filter
	streamer.Filter(func(elem testUser) bool {
		return elem.Age >= 18
	}, func(elem testUser) bool {
		return elem.Name != "wangwu"
	}).Scan(&result)
	expectedResult = []testUser{
		{
			ID:    4,
			Name:  "zhaoliu",
			Age:   25,
			Email: "zhaoliu@xxx.com",
		},
	}
	assertEquals(t, result, expectedResult)
}

func TestStreamerMap(t *testing.T) {
	result := []int{}
	streamer.Filter(func(elem testUser) bool {
		return elem.Age >= 18
	}).Map(func(elem testUser) int {
		return elem.ID
	}).Scan(&result)
	expectedResult := []int{3, 4}

	assertEquals(t, result, expectedResult)
}

func TestStreamerOffset(t *testing.T) {
	result := []testUser{}
	streamer.Offset(1).Scan(&result)
	expectedResult := testData[1:]

	assertEquals(t, result, expectedResult)
}

func TestStreamerLimit(t *testing.T) {
	result := []testUser{}
	streamer.Offset(1).Limit(2).Scan(&result)
	expectedResult := testData[1 : 1+2]

	assertEquals(t, result, expectedResult)
}

func TestStreamerSorted(t *testing.T) {
	result := []int{}
	streamer.Sorted(func(elem1, elem2 testUser) bool {
		return strings.Compare(elem1.Name, elem2.Name) > 0
	}).Map(func(elem testUser) int {
		return elem.ID
	}).Scan(&result)
	expectedResult := []int{4, 1, 3, 2}

	assertEquals(t, result, expectedResult)
}

func TestStreamerForeach(t *testing.T) {
	data := []*testUser{}
	for _, user := range testData {
		// need to clone new user
		newUser := user
		data = append(data, &newUser)
	}
	newStreamerWithData := NewStreamerWithData(data)

	newStreamerWithData.Foreach(func(elem *testUser) {
		elem.Age += 10
	}, func(elem *testUser) {
		elem.Age += 1
	})
	result := []int{}
	newStreamerWithData.Map(func(elem *testUser) int {
		return elem.Age
	}).Scan(&result)
	expectedResult := []int{26, 26, 31, 36}

	assertEquals(t, result, expectedResult)
}

func TestStreamerScan(t *testing.T) {
	result := []testUser{}
	streamer.Scan(&result)
	expectedResult := testData[:]

	assertEquals(t, result, expectedResult)
}

func TestStreamerGroupBy(t *testing.T) {
	result := map[int][]testUser{}
	streamer.GroupBy(func(elem testUser) int {
		return elem.Age
	}, &result)
	expectedResult := map[int][]testUser{
		15: {
			testData[0], testData[1],
		},
		20: {
			testData[2],
		},
		25: {
			testData[3],
		},
	}

	assertEquals(t, result, expectedResult)
}

func TestStreamerToMap(t *testing.T) {
	result := map[int]testUser{}
	streamer.ToMap(func(elem testUser) int {
		return elem.ID
	}, &result)
	expectedResult := map[int]testUser{
		1: testData[0],
		2: testData[1],
		3: testData[2],
		4: testData[3],
	}

	assertEquals(t, result, expectedResult)
}

func TestStreamerFirst(t *testing.T) {
	result := testUser{}
	expectedResult := testData[3]
	exist := streamer.Sorted(func(elem1, elem2 testUser) bool {
		return strings.Compare(elem1.Name, elem2.Name) > 0
	}).First(&result)
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist = streamer.Filter(func(elem testUser) bool {
		return elem.Name == "not found"
	}).First(&result)
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamerLast(t *testing.T) {
	result := testUser{}
	expectedResult := testData[1]
	exist := streamer.Sorted(func(elem1, elem2 testUser) bool {
		return strings.Compare(elem1.Name, elem2.Name) > 0
	}).Last(&result)
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist = streamer.Filter(func(elem testUser) bool {
		return elem.Name == "not found"
	}).First(&result)
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamerIndexAt(t *testing.T) {
	result := testUser{}
	expectedResult := testData[1]
	exist := streamer.IndexAt(1, &result)
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist = streamer.IndexAt(4, &result)
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamerCount(t *testing.T) {
	count := streamer.Count()
	assertEquals(t, len(testData), count)
}
