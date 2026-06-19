package resp

import "fmt"


func MakeErr(msg string) *RespErr {
	return &RespErr{
		Data: []byte(msg),
	}
}

func NotInErr() *RespErr {
	return &RespErr{
		Data: []byte("ERR value is not an integer or out of range"),
	}
}

func ArgNumErr(cmd string) *RespErr {
	msg := fmt.Sprintf("ERR wrong number of arguments to '%s'", cmd)
	return &RespErr{
		Data: []byte(msg),
	}
}

func SyntaxErr() *RespErr {
	return &RespErr{
		Data: []byte("ERR synax error"),
	}
}

func IncompOptionsErr(option1, option2 string) *RespErr {
	msg := fmt.Sprintf("ERR %s and %s options at the same time are not compatible", option1, option2)
	return &RespErr{
		Data: []byte(msg),
	}
}
