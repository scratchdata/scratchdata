package postgrest

type Node struct {
	Type     pegRule
	Value    string
	Parent   *Node
	Children []*Node
}

func (n *Node) TypeString() string {
	return rul3s[n.Type]
}

func PopulateAST(expression string, parent *Node, ast *node32) {
	for {
		if ast == nil {
			break
		}

		child := &Node{}
		child.Type = ast.pegRule
		child.Value = expression[ast.begin:ast.end]

		// Remove when printing
		child.Parent = parent

		parent.Children = append(parent.Children, child)

		PopulateAST(expression, child, ast.up)

		ast = ast.next
	}
}

// parseQueryAST implements ParseQuery, additionally returning the parser's AST
func parseQueryAST(table string, rawquery string) (Postgrest, *node32, error) {
	parser := &PostgrestParser{Buffer: rawquery}
	if err := parser.Init(); err != nil {
		return Postgrest{}, nil, err
	}

	if err := parser.Parse(); err != nil {
		return Postgrest{}, nil, err
	}

	root := &Node{}
	PopulateAST(parser.Buffer, root, parser.AST())

	p := Postgrest{Table: table}
	p.FromAST(root)
	return p, parser.AST(), nil
}

// ParseQuery parses the raw Postgrest compatible query string into a Postgrest object
func ParseQuery(table string, rawquery string) (Postgrest, error) {
	p, _, err := parseQueryAST(table, rawquery)
	return p, err
}
