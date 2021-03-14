package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/kr/pretty"
)

func findSchemaInTable(expr tree.TableExpr) (string, error) {
	switch ast := expr.(type) {
	case *tree.TableName:
		return ast.SchemaName.String(), nil
	case *tree.AliasedTableExpr:
		return findSchemaInTable(ast.Expr)
	}

	return "", fmt.Errorf("Unimplemented table expr: %v", expr)
}

func findAllSchemas(expr tree.Statement) ([]string, error) {
	switch ast := expr.(type) {
	case *tree.Select:
		return findAllSchemas(ast.Select)
	case *tree.SelectClause:
		schemas := []string{}
		for _, table := range ast.From.Tables {
			schema, err := findSchemaInTable(table)
			if err != nil {
				return nil, err
			}
			if schema != "" && schema != `""` {
				schemas = append(schemas, schema)
			}
		}
		return schemas, nil
	}

	return nil, fmt.Errorf("Unimplemented stmt expr: %v", expr)
}

func main() {
	examples := []string{
		`select * from boyz.to_men t, yolozzz.bro x where t.id = 1;`,
		`SELECT  "yolotody19_qa"."some_table".* FROM "yolotody19_qa"."some_table" WHERE ("yolotody19_qa"."some_table"."is_deleted" IN ('t', 'f') OR "yolotody19_qa"."some_table"."is_deleted" IS NULL) AND "yolotody19_qa"."some_table"."user_guid" = 'USR-4f724653-e88d-457b-b151-8c32fb3c51c2'  ORDER BY "yolotody19_qa"."some_table"."id" ASC LIMIT 25 OFFSET 0`,
		`SELECT __user.id, __user.guid FROM "yolos_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`,
		`SELECT * FROM yolo101_shard6.transactions WHERE balance = 13.37`,
		`select * From transactions t where t.id = 1`,
	}

	for _, sql := range examples {
		x, err := parser.Parse(sql)
		if err != nil {
			panic(err)
		}

		fmt.Println("=== SQL:")
		fmt.Println(x.String())
		for _, stmt := range x {
			fmt.Println("==== SCHEMAS:")
			pretty.Println(findAllSchemas(stmt.AST))
		}
		fmt.Println("\n")
	}
}
