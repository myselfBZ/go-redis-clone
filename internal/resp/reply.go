package resp


func OkReply() *SimpleStr {
	return &SimpleStr{
		Data: []byte("OK"),
	}
}
