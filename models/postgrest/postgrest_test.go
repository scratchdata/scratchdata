package postgrest

import (
	"testing"
)

type PostgrestTest struct {
	Expression string
	IsInvalid  bool
	Print      bool
}

var TestCases []PostgrestTest = []PostgrestTest{
	{Expression: "name=eqJohn", IsInvalid: true},
	{Expression: "name=eq.John"},
	{Expression: "name=not.eq.1"},
	{Expression: "name=not.eq.(1)"},
	{Expression: "name=not.eq.(1,2)"},
	{Expression: `name=not.eq.(1,2,"3",3.14,a,b.c,"d.e)","f,","g\"h")`},
	{Expression: `name=eq(any).{1,"2",3}`, Print: true},
}

func TestPEG(t *testing.T) {

	for _, testCase := range TestCases {
		t.Run(testCase.Expression, func(t *testing.T) {
			parser := &PostgrestParser{Buffer: string(testCase.Expression)}
			err := parser.Init()
			if err != nil {
				t.Error(err)
			}

			err = parser.Parse()
			if testCase.IsInvalid && err == nil {
				t.Error("Should have been an error")
			}

			if !testCase.IsInvalid && err != nil {
				t.Error(err)
			}

			if testCase.Print {
				parser.PrettyPrintSyntaxTree(testCase.Expression)
			}
		})
	}

}
