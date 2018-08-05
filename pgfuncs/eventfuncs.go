package pgfuncs

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/andsha/replicagor/structs"
)

func sliceHasInt(slice []int, number int) bool {
	for _, n := range slice {
		if number == n {
			return true
		}
	}

	return false
}

// Generates Postgres queries based on information coming in the event
func GenQuery(event *structs.Event) (string, error) {
	var sql string
	switch event.EventType {
	case structs.INSERT_EVENT:
		for _, vgroup := range event.OldValues {
			sql = fmt.Sprintf("%vINSERT INTO %v.%v (", sql, event.SchemaName, event.TableName)
			enumidxs := make([]int, 0)

			for _, val := range vgroup {
				if val.Value != nil {
					//fmt.Println("1", val.ColumnId, val.Value)
					sql = fmt.Sprintf("%v%v, ", sql, event.Columns[val.ColumnId].Name)
					if event.Columns[val.ColumnId].Type[0:4] == "enum" { // mysql enum column type
						enumidxs = append(enumidxs, val.ColumnId)
					}
				}
			}
			sql = sql[:len(sql)-2] + ") VALUES ("
			for idv, val := range vgroup {
				if val.Value != nil {
					//fmt.Println("excluded from replication:", event.Columns[idv].ExcludedFromReplication)
					if event.Columns[idv].ExcludedFromReplication {
						sql = fmt.Sprintf("%vNULL, ", sql)
					} else {
						switch v := val.Value.(type) {
						case time.Time:
							sql = fmt.Sprintf("%v'%v-%v-%v %v:%v:%v', ", sql, v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second())
						default:
							if sliceHasInt(enumidxs, idv) {
								v, _ := strconv.ParseInt(fmt.Sprintf("%v", val.Value), 10, 16)
								//fmt.Println(v)
								sql = fmt.Sprintf("%v'%v', ", sql, event.Columns[idv].Enum[v-1])
							} else {
								sql = fmt.Sprintf("%v'%v', ", sql, val.Value)
							}
						}
					}
				}
			}
			sql = sql[:len(sql)-2] + "); "
		}
	case structs.UPDATE_EVENT:
		for idg, vgroup := range event.NewValues {
			sql = fmt.Sprintf("%vUPDATE %v.%v SET ", sql, event.SchemaName, event.TableName)

			for idv, val := range vgroup {
				if val.Value != nil {
					if event.Columns[val.ColumnId].ExcludedFromReplication {
						sql = fmt.Sprintf("%v%v = NULL, ", sql, event.Columns[val.ColumnId].Name)
					} else {
						switch v := val.Value.(type) {
						case time.Time:
							sql = fmt.Sprintf("%v%v = '%v-%v-%v %v:%v:%v', ", sql, event.Columns[val.ColumnId].Name, v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second())
						default:
							var n interface{}
							if event.Columns[val.ColumnId].Type[0:4] == "enum" {
								t, _ := strconv.ParseInt(fmt.Sprintf("%v", val.Value), 10, 16)
								n = event.Columns[idv].Enum[t-1]
							} else {
								n = val.Value
							}
							sql = fmt.Sprintf("%v%v = '%v', ", sql, event.Columns[val.ColumnId].Name, n)
						}
					}
				}
			}

			sql = sql[:len(sql)-2] + " WHERE "
			for idv, val := range event.OldValues[idg] {
				if val.Value != nil {
					switch v := val.Value.(type) {
					case time.Time:
						sql = fmt.Sprintf("%v%v = '%v-%v-%v %v:%v:%v' AND ", sql, event.Columns[val.ColumnId].Name, v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second())
					default:
						var n interface{}
						if event.Columns[val.ColumnId].Type[0:4] == "enum" {
							t, _ := strconv.ParseInt(fmt.Sprintf("%v", val.Value), 10, 16)
							n = event.Columns[idv].Enum[t-1]
						} else {
							n = val.Value
						}
						sql = fmt.Sprintf("%v%v = '%v' AND ", sql, event.Columns[val.ColumnId].Name, n)
					}
				}
			}
			sql = sql[:len(sql)-5] + "; "
		}

	case structs.DELETE_EVENT:
		//fmt.Println("delete event", event.OldValues)
		for _, vgroup := range event.OldValues {
			sql = fmt.Sprintf("%vDELETE FROM %v.%v WHERE ", sql, event.SchemaName, event.TableName)
			enumidx := -1

			for idv, val := range vgroup {
				if val.Value != nil {
					switch v := val.Value.(type) {
					case time.Time:
						sql = fmt.Sprintf("%v%v = '%v-%v-%v %v:%v:%v' AND", sql, event.Columns[val.ColumnId].Name, v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second())
					default:
						if idv == enumidx {
							v, _ := strconv.ParseInt(fmt.Sprintf("%v", val.Value), 10, 16)
							//fmt.Println(v)
							sql = fmt.Sprintf("%v'%v' AND ", sql, event.Columns[enumidx].Enum[v-1])
						} else {
							sql = fmt.Sprintf("%v%v = '%v' AND ", sql, event.Columns[val.ColumnId].Name, val.Value)
						}
					}
				}
			}
			sql = sql[:len(sql)-4] + "; "
		}

	default:
		return "", errors.New(fmt.Sprintf("Unknown Event Type %v", event.EventType))
	}

	return sql, nil
}
