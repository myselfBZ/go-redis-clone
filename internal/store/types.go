package store

const (
	Int    = "int"
	String = "string"
)

type Value interface {
	StorageValueType() string
}


type IntValue struct { Data int }

func (v *IntValue) StorageValueType() string {
	return Int
}


type StringValue struct { Data []byte }


func (v *StringValue) StorageValueType() string {
	return String
}

