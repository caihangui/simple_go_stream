# simple_go_stream
A simple golang stream lib
一个通用的golang 流操作库

stream don't support generic, you should convert the interface{} to real upstream data type.

stream包不支持范型，你需要将函数参数中的interface{}手动转化成上游的数据类型

streamv2 support generic, you don't need to convert data type, but the args type of func should be same with upstreaam data type

streamv2支持范型，你不需要手动转化函数中类型，而是可以直接声明成上游数据类型，如果不匹配，那么会报错