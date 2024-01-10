package postgrest

import (
	"encoding/json"
	"testing"
)

func prettyJSON(v any) []byte {
	s, _ := json.MarshalIndent(v, "", "  ")
	return s
}

type sqlTest struct {
	Qry  string
	SQL  string
	Skip string
}

func testSQL(t *testing.T, c sqlTest) {
	t.Helper()
	t.Run(c.Qry, func(t *testing.T) {
		t.Helper()

		if c.Skip != "" {
			t.Skip(c.Skip)
		}

		p, err := ParseQuery("t", c.Qry)
		if err != nil {
			t.Fatal(err)
		}

		sql, err := SQL(p)
		if err != nil {
			t.Fatal(err)
		}

		if sql != c.SQL {
			t.Fatalf("\nPostgrest: %s\n   Query: %#q\nExpected: %#q\n     Got: %#q",
				prettyJSON(p), c.Qry, c.SQL, sql)
		}
	})
}

func TestSQL(t *testing.T) {
	testSQL(t, sqlTest{
		Qry: `name=like(any).{O*,P*}`,
		SQL: `select * from t where name like 'O%' or name like 'P%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=like(all).{O*,P*}`,
		SQL: `select * from t where name like 'O%' and name like 'P%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=like.J*`,
		SQL: `select * from t where name like 'J%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=ilike.j*n`,
		SQL: `select * from t where name ilike 'j%n'`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=eq.90`,
		SQL: `select * from t where grade = 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=lt.90`,
		SQL: `select * from t where grade < 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=lte.90`,
		SQL: `select * from t where grade <= 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=gt.90`,
		SQL: `select * from t where grade > 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=gte.90&student=is.null`,
		SQL: `select * from t where grade >= 90 and student is null`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=gte.90&student=eq."null"`,
		SQL: `select * from t where grade >= 90 and student = 'null'`,
	})
	testSQL(t, sqlTest{
		Qry: `data_fruit=in.("apple")`,
		SQL: `select * from t where data_fruit in ('apple')`,
	})
	testSQL(t, sqlTest{
		Qry: `data_fruit=not.in.("apple")`,
		SQL: `select * from t where data_fruit not in ('apple')`,
	})

	testSQL(t, sqlTest{
		Qry: `name=not.like(any).{O*,P*}`,
		SQL: `select * from t where name not like 'O%' or name not like 'P%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=not.like(all).{O*,P*}`,
		SQL: `select * from t where name not like 'O%' and name not like 'P%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=not.like.J*`,
		SQL: `select * from t where name not like 'J%'`,
	})
	testSQL(t, sqlTest{
		Qry: `name=not.ilike.j*n`,
		SQL: `select * from t where name not ilike 'j%n'`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.eq.90`,
		SQL: `select * from t where not grade = 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.lt.90`,
		SQL: `select * from t where not grade < 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.lte.90`,
		SQL: `select * from t where not grade <= 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.gt.90`,
		SQL: `select * from t where not grade > 90`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.gte.90&student=not.is.null`,
		SQL: `select * from t where not grade >= 90 and student is not null`,
	})
	testSQL(t, sqlTest{
		Qry: `grade=not.gte.90&student=eq."null"`,
		SQL: `select * from t where not grade >= 90 and student = 'null'`,
	})
	testSQL(t, sqlTest{
		Qry: `data_fruit=not.in.("apple")`,
		SQL: `select * from t where data_fruit not in ('apple')`,
	})
	testSQL(t, sqlTest{
		Qry: `data_fruit=not.in.("apple")`,
		SQL: `select * from t where data_fruit not in ('apple')`,
	})
}
