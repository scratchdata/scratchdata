package postgrest

import (
	"fmt"
	"scratchdata/util"
	"strings"
)

var (
	// sqlKeywords is a list of known SQL keywords that should not be treated as strings
	sqlKeywords = map[string]bool{
		"true":  true,
		"false": true,
		"null":  true,
	}

	sqlLikeReplacer = strings.NewReplacer(
		// we're using printf, so escape percent signs
		"%", "%%",
		// replace `*` from Postgrest queries with the SQL equivalent `%`
		"*", "%%",
	)
)

type sqlOpertor struct {
	// SQL is the SQL operator value to output
	SQL string

	// NeedsParens causes the operands to be wrapped in parentheses, even if there is only 1
	NeedsParens bool

	// ReplaceAsterisk causes `*` to be replaced with `%` in the operand's output
	ReplaceAsterisk bool

	InlineNot bool
}

type sqlFlag uint32

// AppendSQL converts query to SQL and appends it to b
func AppendSQL(b *util.StringBuffer, query Postgrest) error {
	b.Printf(`select * from`).Space().SQLIdent(query.Table)

	if len(query.Filters) != 0 {
		b.Space().Printf(`where`)
		for i, f := range query.Filters {
			// TODO: this might need to change when `and.` and `or.` are supported
			b.PrintfIf(i > 0, ` and`)
			if err := filterToSQL(b, f); err != nil {
				return err
			}
		}
	}

	if query.Limit > 0 {
		b.Space().Printf(`limit %d`, query.Limit)
	}

	if query.Offset > 0 {
		b.Space().Printf(`offset %d`, query.Offset)
	}

	if len(query.Order) != 0 {
		b.Space().Printf(`order by`)
		for i, f := range query.Filters {
			b.PrintfIf(i > 0, `, `)
			if err := filterToSQL(b, f); err != nil {
				return err
			}
		}
	}

	return nil
}

// SQL convert query to a SQL string
func SQL(query Postgrest) (string, error) {
	b := &util.StringBuffer{}
	if err := AppendSQL(b, query); err != nil {
		return "", err
	}
	return b.String(), nil
}

func orderToSQL(b *util.StringBuffer, o *Order) error {
	b.Space().SQLIdent(o.Column)

	switch o.Direction {
	case "":
	case "asc", "desc":
		b.Space().Printf(o.Column)
	default:
		return fmt.Errorf("Unknown order by direction %#q", o.Direction)
	}

	switch o.NullDirection {
	case "":
	case "nullsfirst":
		b.Printf(" nulls first")
	case "nullslast":
		b.Printf(" nulls last")
	default:
		return fmt.Errorf("Unknown nulls direction %#q", o.NullDirection)
	}

	return nil
}

func filterToSQL(b *util.StringBuffer, f *Filter) error {
	// contents of the column section
	column := f.Field
	// contents of the operants section
	operands := f.Operands
	// whole filter is degated
	negated := f.Not != ""
	// the SQL operator
	operator := f.Operator
	// wrap the operands in parentheses
	// AnyAll splits the operands, so they don't need parens by default
	needsParens := len(f.Operands) > 1 && f.AnyAll == ""
	// replace the contents of the operand
	var replacer *strings.Replacer

	switch operator {
	case "eq":
		operator = "="
	case "gt":
		operator = ">"
	case "gte":
		operator = ">="
	case "lt":
		operator = "<"
	case "lte":
		operator = "<="
	case "neq":
		operator = "<>"
	case "like", "ilike":
		replacer = sqlLikeReplacer
		if negated {
			negated = false
			operator = "not " + operator
		}
	case "in":
		needsParens = true
		if negated {
			negated = false
			operator = "not in"
		}
	case "is":
		if negated {
			negated = false
			operator = "is not"
		}
	default:
		return fmt.Errorf("Unknown operator %#q", operator)
	}

	columnToSQL := func() {
		if negated {
			b.Space().Printf("not")
		}
		b.Space().Printf(column)
		b.Space().Printf(operator)
	}

	operandToSQL := func(fo FilterOperand) {
		val := fo.Value
		if replacer != nil {
			val = replacer.Replace(val)
		}

		switch {
		case fo.Type == FilterOperandQuoted:
			// if the input was `"null"`, we don't want to output `null`
			b.SQLString(val)
		case fo.Type == FilterOperandNumber:
			b.Printf(val)
		case sqlKeywords[strings.ToLower(val)]:
			b.Printf(val)
		default:
			b.SQLString(val)
		}
	}

	switch {
	case f.AnyAll != "":
		sep := " or"
		if f.AnyAll == "all" {
			sep = " and"
		}
		for i, fo := range f.Operands {
			b.PrintfIf(i > 0, sep)
			columnToSQL()
			b.Space()
			b.PrintfIf(needsParens, "(")
			operandToSQL(fo)
			b.PrintfIf(needsParens, ")")
		}
	default:
		columnToSQL()
		b.Space()
		b.PrintfIf(needsParens, "(")
		for i, fo := range operands {
			b.PrintfIf(i > 0, ", ")
			operandToSQL(fo)
		}
		b.PrintfIf(needsParens, ")")
	}

	return nil
}
