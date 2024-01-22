package postgrest

import "fmt"

type Node struct {
	Type     pegRule
	Value    string
	Parent   *Node
	Children []*Node
}

func CompactTree(node *Node) *Node {
	if node == nil {
		return nil
	}

	// oldNode := node
	// If the current node is part of a linear chain, skip to the last node in the chain
	for len(node.Children) == 1 {
		node = node.Children[0]
	}

	// Create a new node instance
	newNode := &Node{
		// Type: oldNode.Type,
		Type:  node.Type,
		Value: node.Value,
	}

	// Recursively compact the children
	for _, child := range node.Children {
		newNode.Children = append(newNode.Children, CompactTree(child))
	}

	return newNode
}

func PrintTree(node *Node, level int) {
	if node == nil {
		return
	}

	fmt.Printf("%*s%s %s\n", level, "", node.TypeString(), node.Value)
	for _, child := range node.Children {
		PrintTree(child, level+1)
	}
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
		// child.Parent = parent

		parent.Children = append(parent.Children, child)

		PopulateAST(expression, child, ast.up)

		ast = ast.next
	}
}
