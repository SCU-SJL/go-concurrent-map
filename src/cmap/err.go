package cmap

import "fmt"

type IllegalParameterError struct {
	msg string
}

func newIllegalParameterError(errMsg string) IllegalParameterError {
	return IllegalParameterError{
		msg: fmt.Sprintf("concurrent map: illegal parameter: %s", errMsg),
	}
}

func (ipe IllegalParameterError) Error() string {
	return ipe.msg
}

type IllegalPairTypeError struct {
	msg string
}

func newIllegalPairTypeError(pair Pair) IllegalPairTypeError {
	return IllegalPairTypeError{
		msg: fmt.Sprintf("concurrent map: illegal pair type: %T", pair),
	}
}

func (ipte IllegalPairTypeError) Error() string {
	return ipte.msg
}

type PairRedistributorError struct {
	msg string
}

func newPairRedistributorError(errMsg string) PairRedistributorError {
	return PairRedistributorError{
		msg: fmt.Sprintf("concurrent map: failing pair redistribution: %s", errMsg),
	}
}

func (pre PairRedistributorError) Error() string {
	return pre.msg
}
