package resp

import (
	"fmt"
	"strconv"
	"strings"
)

// template strings should only be allocated once at compile time.

// this file implements data structure for resp
var (
	CRLF = "\r\n"
	NIL  = []byte("$-1\r\n") // Null arrays
)

type RedisData interface {
	ToBytes() []byte  // return resp transfer format data
	ByteData() []byte // return byte data
	String() string   // return resp string
}

type StringData struct {
	data string
}

type BulkData struct {
	data []byte
}

type IntData struct {
	data int64
}

type ErrorData struct {
	data string
}

// ArrayData do not implement ByteData()
type ArrayData struct {
	data []RedisData
}

type PlainData struct {
	data string
}

// 1. bulkData
func MakeBulkData(data []byte) *BulkData {
	return &BulkData{
		data: data,
	}
}

// $表示BulkData，后接len
func (r *BulkData) ToBytes() []byte {
	// return nil
	if r.data == nil {
		return NIL
	}
	// return data string
	return []byte("$" + strconv.Itoa(len(r.data)) + CRLF + string(r.data) + CRLF)
}

func (r *BulkData) Data() []byte {
	return r.data
}

func (r *BulkData) ByteData() []byte {
	return r.data
}

func (r *BulkData) String() string {
	return string(r.data)
}

// 2. stringData
func MakeStringData(data string) *StringData {
	return &StringData{
		data: data,
	}
}

// +表示StringData
func (r *StringData) ToBytes() []byte {
	return []byte("+" + r.data + CRLF)
}

func (r *StringData) Data() string {
	return r.data
}
func (r *StringData) ByteData() []byte {
	return []byte(r.data)
}

func (r *StringData) String() string {
	return r.data
}

// 3. IntData
func MakeIntData(data int64) *IntData {
	return &IntData{
		data: data,
	}
}

// :表示IntData
func (r *IntData) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.data, 10) + CRLF)
}

func (r *IntData) Data() int64 {
	return r.data
}

func (r *IntData) ByteData() []byte {
	return []byte(strconv.FormatInt(r.data, 10))
}

func (r *IntData) String() string {
	return strconv.FormatInt(r.data, 10)
}

// 4. ErrorData
func MakeErrorData(data ...string) *ErrorData {
	errMsg := ""
	for _, s := range data {
		errMsg += s
	}
	return &ErrorData{
		data: errMsg,
	}
}

func MakeWrongType() *ErrorData {
	return &ErrorData{data: fmt.Sprintf("WRONGTYPE Operation against a key holding the wrong kind of value")}
}

func MakeWrongNumberArgs(name string) *ErrorData {
	return &ErrorData{data: fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)}
}

// -表示ErrorData
func (r *ErrorData) ToBytes() []byte {
	return []byte("-" + r.data + CRLF)
}

func (r *ErrorData) Error() string {
	return r.data
}

func (r *ErrorData) Data() string {
	return r.data
}

func (r *ErrorData) ByteData() []byte {
	return []byte(r.data)
}

func (r *ErrorData) String() string {
	return r.data
}

// 5.ArrayData
func MakeArrayData(data []RedisData) *ArrayData {
	return &ArrayData{
		data: data,
	}
}

func MakeEmptyArrayData() *ArrayData {
	return &ArrayData{
		data: []RedisData{},
	}
}

// *表示ArrayData
func (r *ArrayData) ToBytes() []byte {
	if r.data == nil {
		return []byte("*-1\r\n")
	}

	res := []byte("*" + strconv.Itoa(len(r.data)) + CRLF)
	for _, v := range r.data {
		res = append(res, v.ToBytes()...)
	}
	return res
}
func (r *ArrayData) Data() []RedisData {
	return r.data
}

func (r *ArrayData) ToCommand() [][]byte {
	res := make([][]byte, 0)
	for _, v := range r.data {
		res = append(res, v.ByteData())
	}
	return res
}

func (r *ArrayData) ToStringCommand() []string {
	res := make([]string, 0, len(r.data))
	for _, v := range r.data {
		res = append(res, string(v.ByteData()))
	}
	return res
}

func (r *ArrayData) String() string {
	return strings.Join(r.ToStringCommand(), " ")
}

// ByteData is discarded. Use ToCommand() instead.
func (r *ArrayData) ByteData() []byte {
	res := make([]byte, 0)
	for _, v := range r.data {
		res = append(res, v.ByteData()...)
	}
	return res
}

func MakePlainData(data string) *PlainData {
	return &PlainData{
		data: data,
	}
}
func (r *PlainData) ToBytes() []byte {
	return []byte(r.data + CRLF)
}
func (r *PlainData) Data() string {
	return r.data
}

func (r *PlainData) String() string {
	return r.data
}

func (r *PlainData) ByteData() []byte {
	return []byte(r.data)
}
