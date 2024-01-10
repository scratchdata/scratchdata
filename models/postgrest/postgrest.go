//go:generate go run golang.org/x/tools/cmd/stringer -type=FilterOperandType

package postgrest

import (
	"strconv"
	"unicode"
	"unicode/utf8"
)

const (
	_ FilterOperandType = iota
	FilterOperandQuoted
	FilterOperandNumber
)

type Postgrest struct {
	Table   string
	Filters []*Filter
	Limit   int
	Offset  int
	Order   []*Order
	// LogicalQuery
}

func (p *Postgrest) FromAST(node *Node) {
	switch node.Type {
	case ruleFilter:
		filter := &Filter{}
		filter.FromAST(node)
		p.Filters = append(p.Filters, filter)
	default:
		for _, child := range node.Children {
			p.FromAST(child)
		}
	}
}

type Order struct {
	Column        string
	Direction     string
	NullDirection string
}

type FilterOperandType int

func (f FilterOperandType) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

type FilterOperand struct {
	Value string
	Type  FilterOperandType
}

type Filter struct {
	Field    string
	Not      string
	Operator string
	Operands []FilterOperand
	AnyAll   string
}

func (filter *Filter) FromAST(n *Node) {

	for _, child := range n.Children {

		if child.Type == ruleColumnName {
			filter.Field = child.Value
		}

		if child.Type == rulePredicate {
			for _, pred := range child.Children {
				switch pred.Type {
				case ruleNot:
					filter.Not = "not"
				case ruleAnyAll:
					filter.AnyAll = pred.Value
				case ruleOperator:
					filter.Operator = pred.Value
				case ruleOperand:
					if pred.Children[0].Type == ruleVectorOperand {
						for _, operand := range pred.Children[0].Children {
							filter.Operands = append(filter.Operands, filterOperand(operand))
						}
					} else if pred.Children[0].Type == ruleScalarOperand {
						filter.Operands = append(filter.Operands, filterOperand(pred.Children[0]))
					}
				case ruleListOperand:
					for _, operand := range pred.Children {
						filter.Operands = append(filter.Operands, filterOperand(operand))
					}
				}
			}
		}
	}
}

func filterOperand(n *Node) FilterOperand {
	fo := FilterOperand{
		Value: n.Value,
	}
	if s, err := strconv.Unquote(n.Value); err == nil {
		fo.Value = s
		fo.Type = FilterOperandQuoted
	} else if r, _ := utf8.DecodeRuneInString(fo.Value); unicode.IsDigit(r) {
		fo.Type = FilterOperandNumber
	}
	return fo
}
