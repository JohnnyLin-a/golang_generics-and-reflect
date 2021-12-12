package main

import (
	"errors"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

func reflectKeyExists(v *reflect.Value, keyValue *reflect.Value) *reflect.Value {
	// Check if key exists
	for _, currentKey := range v.MapKeys() {
		if (currentKey.Kind() == reflect.Int && keyValue.Kind() == reflect.Int && keyValue.Int() == currentKey.Int()) ||
			(currentKey.Kind() == reflect.String && keyValue.Kind() == reflect.String && keyValue.String() == currentKey.String()) {
			return &currentKey
		}
	}
	return nil
}

/*
	RecordReduceWithKey sorts records and reduces them by keys specified in the indexing param.

	The pointer passed in outputRecord is directly modified (Similar to json.Unmarshal).
*/
func RecordReduceWithKey[T any, Out any](records []T, outputRecord *Out, indexing ...string) error {
	if outputRecord == nil {
		return errors.New("outputRecord cannot be nil pointer")
	}
	output := reflect.Indirect(reflect.ValueOf(outputRecord))

	for _, record := range records {
		currentLevel := output
		var tPropValue reflect.Value

		// Traverse into indexing
		for i, prop := range indexing {
			// Check if prop is exported
			if prop[0:1] != strings.ToUpper(prop[0:1]) {
				return errors.New("prop must be pubic: " + prop)
			}

			// Validate T's prop type
			tPropValue = reflect.ValueOf(record).FieldByName(prop)
			if !tPropValue.IsValid() {
				return errors.New("invalid prop name of " + prop)
			}
			tPropKind := reflect.ValueOf(record).FieldByName(prop).Kind()
			switch tPropKind {
			case reflect.Int, reflect.String:
				break
			default:
				return errors.New("invalid prop type, must be string, int, but got " + tPropKind.String())
			}

			// Validate map type
			if currentLevel.Kind() != reflect.Map {
				return errors.New("output is not a map")
			}

			// Check if key exists
			var found *reflect.Value = reflectKeyExists(&currentLevel, &tPropValue)

			// Create key if key does not exist
			if found == nil && len(indexing) != i+1 {
				if currentLevel.Type().Elem().Kind() == reflect.Map {
					nextMapType := currentLevel.Type().Elem()
					if tPropKind != currentLevel.Type().Key().Kind() {
						return errors.New("Key datatype does not match for map[" + currentLevel.Type().Key().Kind().String() + "]" + currentLevel.Type().Elem().Kind().String() + ", got " + tPropKind.String())
					}
					currentLevel.SetMapIndex(tPropValue, reflect.MakeMap(nextMapType))
					found = reflectKeyExists(&currentLevel, &tPropValue)
				} else {
					return errors.New("cannot create new map for next level. There is/are " + strconv.Itoa(len(indexing)) + " indexing key(s), therefore should have the same amount of map(s)")
				}
			}

			// Check if final level is slice or not and add record
			if len(indexing) == i+1 {
				// Validate key kind
				if currentLevel.Type().Key().Kind() != tPropKind {
					return errors.New("map key datatype incorrect. map key type of " + currentLevel.Type().Key().Kind().String() + " but got " + tPropKind.String() + " prop")
				}

				if currentLevel.Type().Elem().Kind() == reflect.Slice {
					found = reflectKeyExists(&currentLevel, &tPropValue)

					if found == nil {
						nextSliceType := currentLevel.Type().Elem()
						if reflect.TypeOf(record) != nextSliceType.Elem() {
							return errors.New("cannot assign slice's value of datatype " + nextSliceType.Elem().String() + " for record of type " + reflect.TypeOf(record).String())
						}
						currentLevel.SetMapIndex(tPropValue, reflect.Append(reflect.MakeSlice(nextSliceType, 0, 0), reflect.ValueOf(record)))
					} else {
						currentLevel.SetMapIndex(tPropValue, reflect.Append(currentLevel.MapIndex(*found), reflect.ValueOf(record)))
					}
				} else {
					if reflect.TypeOf(record) != currentLevel.Type().Elem() {
						return errors.New("cannot assign map's value of datatype " + currentLevel.Type().Elem().String() + " for record of type " + reflect.TypeOf(record).String())
					}
					currentLevel.SetMapIndex(tPropValue, reflect.ValueOf(record))
				}
			}

			// Update currentLevel
			if found != nil {
				currentLevel = currentLevel.MapIndex(*found)
			}
		}
	}
	return nil
}

type record1 struct {
	Record1_id int
	Val1       string
}
type recordx struct{}

func main() {
	records := []record1{{1, "val"}, {1, "val"}, {3, "val"}, {4, "val2"}, {3, "val2"}}

	output := make(map[string]map[int][]record1)

	err := RecordReduceWithKey(records, &output, "Val1", "Record1_id")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	log.Println(output)
	/*
		expected output:
		output
			["val"]
				[1]
					[{1, "val"}, {1, "val"}]
				[3]
					[{3, "val"}]
			["val2"]
				[3]
					[{3, "val2"}]
				[4]
					[{4, "val2"}]
	*/
}
