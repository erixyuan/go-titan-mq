package tools

import "reflect"

func ContainsInt(arr interface{}, elem interface{}) bool {
	slice := reflect.ValueOf(arr)
	if slice.Kind() != reflect.Slice {
		panic("ContainsInt() only works on slices")
	}
	for i := 0; i < slice.Len(); i++ {
		if reflect.DeepEqual(slice.Index(i).Interface(), elem) {
			return true
		}
	}
	return false
}
