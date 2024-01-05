package postgrest

import "strconv"

type Postgrest struct {
	Filters []*Filter
}

func (p *Postgrest) FromAST(node *Node) {
	switch node.Type {
	case "Filter":
		filter := &Filter{}
		filter.FromAST(node)
		p.Filters = append(p.Filters, filter)
	default:
		for _, child := range node.Children {
			p.FromAST(child)
		}
	}
}

type Filter struct {
	Field    string
	Not      string
	Operator string
	Operands []string
	AnyAll   string
}

func (filter *Filter) FromAST(n *Node) {

	for _, child := range n.Children {

		if child.Type == "ColumnName" {
			filter.Field = child.Value
		}

		if child.Type == "Predicate" {
			for _, pred := range child.Children {
				if pred.Type == "Not" {
					filter.Not = "NOT"
				}
				if pred.Type == "Operator" {
					filter.Operator = pred.Value
				}

				if pred.Type == "Operand" {
					if pred.Children[0].Type == "VectorOperand" {
						for _, operand := range pred.Children[0].Children {
							unquoted, err := strconv.Unquote(operand.Value)
							if err == nil {
								filter.Operands = append(filter.Operands, unquoted)
							} else {
								filter.Operands = append(filter.Operands, operand.Value)
							}
						}

					} else if pred.Children[0].Type == "ScalarOperand" {
						unquoted, err := strconv.Unquote(pred.Children[0].Value)
						if err == nil {
							filter.Operands = append(filter.Operands, unquoted)
						} else {
							filter.Operands = append(filter.Operands, pred.Children[0].Value)
						}
					}

				}
			}
		}
	}
}
