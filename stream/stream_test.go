package stream

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

//func (u *testUser) String() string {
//	return fmt.Sprintf("{\"id\":%d,\"name\":\"%s\",\"age\":%d,\"email\":\"%s\"}",u.ID,u.Name,u.Age,u.Email)
//}

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
	streamer, _ = NewStreamerWithData(testData)
}

// 仅限用于test，实际使用是reflect.DeepEqual的性能不行
func assertEquals(t *testing.T, result, expectedResult interface{}) {
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected_result: %v , but return %v", expectedResult, result)
	}
}

func TestNewStreamerWithData(t *testing.T) {
	_, err := NewStreamerWithData(testData)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamer_Filter(t *testing.T) {
	result := []testUser{}
	err := streamer.Filter(func(elem interface{}) bool {
		return elem.(testUser).Age >= 18
	}).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestStreamer_Map(t *testing.T) {
	result := []int{}
	err := streamer.Filter(func(elem interface{}) bool {
		return elem.(testUser).Age >= 18
	}).Map(func(elem interface{}) interface{} {
		return elem.(testUser).ID
	}).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := []int{3, 4}

	assertEquals(t, result, expectedResult)
}

func TestStreamer_Offset(t *testing.T) {
	result := []testUser{}
	err := streamer.Offset(1).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := testData[1:]

	assertEquals(t, result, expectedResult)
}

func TestStreamer_Limit(t *testing.T) {
	result := []testUser{}
	err := streamer.Offset(1).Limit(2).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := testData[1 : 1+2]

	assertEquals(t, result, expectedResult)
}

func TestStreamer_Sorted(t *testing.T) {
	result := []int{}
	err := streamer.Sorted(func(elem1, elem2 interface{}) bool {
		return strings.Compare(elem1.(testUser).Name, elem2.(testUser).Name) > 0
	}).Map(func(elem interface{}) interface{} {
		return elem.(testUser).ID
	}).Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := []int{4, 1, 3, 2}

	assertEquals(t, result, expectedResult)
}

func TestStreamer_Foreach(t *testing.T) {
	result := []int{}
	err := streamer.Foreach(func(elem interface{}) error {
		result = append(result, elem.(testUser).Age+10)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := []int{25, 25, 30, 35}

	assertEquals(t, result, expectedResult)
}

func TestStreamer_Scan(t *testing.T) {
	result := []testUser{}
	err := streamer.Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	expectedResult := testData[:]

	assertEquals(t, result, expectedResult)
}

func TestStreamer_GroupBy(t *testing.T) {
	result := map[int][]testUser{}
	err := streamer.GroupBy(func(elem interface{}) interface{} {
		return elem.(testUser).Age
	}, &result)
	if err != nil {
		t.Fatal(err)
	}
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

func TestStreamer_First(t *testing.T) {
	result := testUser{}
	expectedResult := testData[3]
	exist, err := streamer.Sorted(func(elem1, elem2 interface{}) bool {
		return strings.Compare(elem1.(testUser).Name, elem2.(testUser).Name) > 0
	}).First(&result)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist, err = streamer.Filter(func(elem interface{}) bool {
		return elem.(testUser).Name == "not found"
	}).First(&result)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamer_Last(t *testing.T) {
	result := testUser{}
	expectedResult := testData[1]
	exist, err := streamer.Sorted(func(elem1, elem2 interface{}) bool {
		return strings.Compare(elem1.(testUser).Name, elem2.(testUser).Name) > 0
	}).Last(&result)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist, err = streamer.Filter(func(elem interface{}) bool {
		return elem.(testUser).Name == "not found"
	}).First(&result)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamer_IndexAt(t *testing.T) {
	result := testUser{}
	expectedResult := testData[1]
	exist, err := streamer.IndexAt(1, &result)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Errorf("excepted result is %v, but not found", expectedResult)
	}
	assertEquals(t, result, expectedResult)

	exist, err = streamer.IndexAt(4, &result)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Errorf("excepted not found, but return %v", result)
	}
}

func TestStreamer_Count(t *testing.T) {
	count, err := streamer.Count()
	if err != nil {
		t.Fatal(err)
	}
	assertEquals(t, len(testData), count)
}
