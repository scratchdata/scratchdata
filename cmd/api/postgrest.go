package api

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

type Node struct {
	Type     string // Predicate, Scalar
	Field    string
	Operator string
	Children []*Node
}

func (n *Node) AddChild(child *Node) {
	n.Children = append(n.Children, child)
}

func (n *Node) ToSQL() string {
	if n.Type == "Scalar" {
		return n.Field
	}

	if n.Type == "Predicate" {
		switch n.Operator {
		case "and":
			predicates := make([]string, len(n.Children))
			for i, child := range n.Children {
				predicates[i] = child.ToSQL()
			}
			return "(" + strings.Join(predicates, " AND ") + ")"
		case "or":
			predicates := make([]string, len(n.Children))
			for i, child := range n.Children {
				predicates[i] = child.ToSQL()
			}
			return "(" + strings.Join(predicates, " OR ") + ")"
		case "in":
			predicates := make([]string, len(n.Children))
			for i, child := range n.Children {
				predicates[i] = child.ToSQL()
			}
			return fmt.Sprintf("(%s IN (%s))", n.Field, strings.Join(predicates, ","))
		case "not":
			return "(NOT (" + n.Children[0].ToSQL() + "))"
		case "is":
			return fmt.Sprintf("(%s = %s)", n.Field, n.Children[0].ToSQL())
		case "gt":
			return fmt.Sprintf("(%s > %s)", n.Field, n.Children[0].ToSQL())
		case "gte":
			return fmt.Sprintf("(%s >= %s)", n.Field, n.Children[0].ToSQL())
		case "lt":
			return fmt.Sprintf("(%s < %s)", n.Field, n.Children[0].ToSQL())
		case "lte":
			return fmt.Sprintf("(%s <= %s)", n.Field, n.Children[0].ToSQL())
		}
	}

	return ""
}

func (i *API) predicateToSQL() {
	// grade >= 90
	// Node{"Predicate", "grade", "GT", [Node{"Scalar", 90}]}
	// Node{"Predicate", "", "AND", [Node{"PREDICATE", ...}]}
	// Node{"Predicate", "", "NOT", [Node{"PREDICATE", ...}]}
	// Node{"Predicate", "age", "IN", [Node{"Scalar", ...}]}

}

func (i *API) queryToSQL() {
	selectFields := []string{"a", "b"}
	// selectFields := []string{"a", "b"}

	log.Print(selectFields)

}

func (i *API) PostgrestQuery(c *fiber.Ctx) error {
	log.Trace().Interface("headers", c.GetReqHeaders()).Send()
	log.Print(c.AllParams())
	log.Print(c.Queries())

	n := &Node{
		Type:  "Predicate",
		Field: "grade",
	}
	n.AddChild(&Node{Type: "Scalar", Field: "5"})

	log.Print(n.ToSQL())

	return nil
}
