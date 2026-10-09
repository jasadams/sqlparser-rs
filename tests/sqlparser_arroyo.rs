// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]

use sqlparser::ast::{
    BinaryOperator, ColumnOption, DataType, Expr, Function, Ident, SelectItem, SetExpr, Statement,
    TableConstraint, Value,
};
use sqlparser::dialect::{ArroyoDialect, GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use sqlparser::test_utils;
use sqlparser::test_utils::TestedDialects;
use sqlparser::tokenizer::{Location, Span};

fn arroyo_dialects() -> TestedDialects {
    TestedDialects::new(vec![Box::new(ArroyoDialect {})])
}

/// Parse `sql` as a single expression under the Arroyo dialect.
fn parse_arroyo_expr(sql: &str) -> Expr {
    arroyo_dialects().run_parser_method(sql, |parser| {
        parser
            .parse_expr()
            .unwrap_or_else(|err| panic!("failed to parse {sql}: {err}"))
    })
}

/// Parse `sql` as an expression, assert that `Display` produces the exact
/// same SQL, and that re-parsing the displayed SQL yields an equal AST.
fn assert_arroyo_expr_roundtrip(sql: &str) -> Expr {
    let expr = parse_arroyo_expr(sql);
    let displayed = expr.to_string();
    assert_eq!(sql, displayed, "Display must round-trip for {sql}");
    let reparsed = parse_arroyo_expr(&displayed);
    assert_eq!(
        expr, reparsed,
        "re-parsing the Display of {sql} must yield the same AST"
    );
    expr
}

/// Parse `sql` as a single statement, assert that `Display` produces the
/// exact same SQL, and that re-parsing the displayed SQL yields an equal
/// AST.
fn assert_arroyo_stmt_roundtrip(sql: &str) -> Statement {
    let statements = arroyo_dialects()
        .parse_sql_statements(sql)
        .unwrap_or_else(|err| panic!("failed to parse {sql}: {err}"));
    assert_eq!(1, statements.len(), "expected a single statement for {sql}");
    let statement = statements.into_iter().next().unwrap();
    let displayed = statement.to_string();
    assert_eq!(sql, displayed, "Display must round-trip for {sql}");
    let reparsed = arroyo_dialects()
        .parse_sql_statements(&displayed)
        .unwrap_or_else(|err| panic!("failed to re-parse Display of {sql}: {err}"));
    assert_eq!(1, reparsed.len());
    assert_eq!(
        statement, reparsed[0],
        "re-parsing the Display of {sql} must yield the same AST"
    );
    statement
}

/// Parse `sql` under the Arroyo dialect and assert that it is rejected with
/// a `ParserError` mentioning `expected_in_error`.
fn assert_arroyo_parse_error(sql: &str, expected_in_error: &str) {
    let err = Parser::parse_sql(&ArroyoDialect {}, sql)
        .expect_err(&format!("expected a parse error for: {sql}"));
    let msg = err.to_string();
    assert!(
        msg.contains(expected_in_error),
        "expected the error for {sql:?} to contain {expected_in_error:?}, but got: {msg}"
    );
}

#[test]
fn test_watermark_with_expr() {
    let sql = "CREATE TABLE orders (
        customer_id INT,
        order_id INT,
        date_string TEXT,
        timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string as TIMESTAMP)),
        WATERMARK FOR timestamp AS timestamp + 5
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("timestamp"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(
                    test_utils::number("5")
                        .with_span(Span::new(Location::new(5, 4), Location::new(5, 10)))
                )),
            }),
        }]
    );
}

#[test]
fn test_watermark_without_expr() {
    let sql = "CREATE TABLE users (
        customer_id INT,
        timestamp TIMESTAMP,
        WATERMARK FOR timestamp
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: None,
        }]
    );
}

#[test]
fn test_metadata_field() {
    let sql = "CREATE TABLE logs (
        id TEXT,
        kafka_topic STRING METADATA FROM 'topic',
        log TEXT
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(ct.columns.len(), 3);

    // Check the middle column with METADATA FROM
    let column = &ct.columns[1];
    assert_eq!(column.name, Ident::new("kafka_topic"));

    // Check for the METADATA FROM option
    let mut found_metadata = false;
    for option_def in &column.options {
        if let ColumnOption::MetadataField(key, _) = &option_def.option {
            found_metadata = true;
            assert_eq!(key, "topic");
        }
    }

    assert!(
        found_metadata,
        "Expected METADATA FROM option in column definition"
    );
}

#[test]
fn test_iceberg_partitioned_by() {
    let sql = "CREATE TABLE ice (
        ts TIMESTAMP NOT NULL,
        id INT NOT NULL,
        favorite_color TEXT
    ) WITH (
        connector = 'iceberg',
        format = 'parquet',
        table_name = 'arroyo_test'
    ) PARTITIONED BY (
        hour(ts),
        bucket(32, id),
        truncate(8, favorite_color)
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    // Verify basic structure
    assert_eq!(ct.name.to_string(), "ice");
    assert_eq!(ct.columns.len(), 3);

    // Verify arroyo_partitions is present
    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions to be Some");
    assert_eq!(partitions.len(), 3);

    // Check each partition transform
    // hour(ts)
    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "hour");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 1);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for hour(ts)"),
    }

    // bucket(32, id)
    match &partitions[1] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "bucket");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for bucket(32, id)"),
    }

    // truncate(8, favorite_color)
    match &partitions[2] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "truncate");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for truncate(8, favorite_color)"),
    }

    // Test round-trip: the formatted output should parse back to the same structure
    let formatted = ct.to_string();
    let reparsed = Parser::parse_sql(&ArroyoDialect {}, &formatted).unwrap();
    let Statement::CreateTable(ct2) = reparsed.get(0).unwrap() else {
        panic!("not create table on reparse")
    };

    assert_eq!(ct.arroyo_partitions, ct2.arroyo_partitions);
}

#[test]
fn test_iceberg_partitioned_by_single() {
    let sql = "CREATE TABLE events (
        event_time TIMESTAMP
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (day(event_time))";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "day");
        }
        _ => panic!("Expected Function for day(event_time)"),
    }
}

#[test]
fn test_iceberg_partitioned_by_identity() {
    // Test identity transform (just a column name)
    let sql = "CREATE TABLE data (
        region TEXT,
        value INT
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (region)";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Identifier(ident) => {
            assert_eq!(ident.value, "region");
        }
        _ => panic!("Expected Identifier for region"),
    }
}

#[test]
fn test_sql_json_extraction_roundtrip() {
    for sql in [
        "JSON_EXISTS(e.payload, '$.traits.name')",
        "JSON_VALUE(e.payload, '$.traits.name')",
        "JSON_VALUE(e.payload, '$.traits.name' RETURNING VARCHAR)",
        "JSON_VALUE(e.payload, '$.traits.steam_wishlisted' RETURNING BOOLEAN)",
        "JSON_VALUE(e.payload, '$.traits.quality_score' RETURNING DOUBLE PRECISION)",
        "JSON_QUERY(e.payload, '$.traits.linked_accounts')",
        "JSON_EXISTS((a + b) * c, '$.x')",
        "JSON_VALUE(t.doc, '$.a' RETURNING VARCHAR)",
        // Embedded single quote in the path
        "JSON_VALUE(x, '$.a''b')",
        // The ticket's filter path, with brackets, spaces, parens and
        // double quotes inside the single-quoted literal
        "JSON_VALUE(x, '$.identities[*] ? (@.identity_type == \"email\").value')",
    ] {
        assert_arroyo_expr_roundtrip(sql);
    }
}

#[test]
fn test_sql_json_extraction_ast_shapes() {
    // JSON_EXISTS keeps the document and path
    let expr = assert_arroyo_expr_roundtrip("JSON_EXISTS(e.payload, '$.traits.name')");
    let Expr::JsonExists { doc, path } = expr else {
        panic!("expected Expr::JsonExists, got {expr:?}");
    };
    assert_eq!("$.traits.name", path);
    assert!(
        matches!(*doc, Expr::CompoundIdentifier(_)),
        "expected e.payload as a compound identifier, got {doc:?}"
    );

    // JSON_QUERY keeps the document and path
    let expr = assert_arroyo_expr_roundtrip("JSON_QUERY(e.payload, '$.traits.linked_accounts')");
    let Expr::JsonQuery { doc, path } = expr else {
        panic!("expected Expr::JsonQuery, got {expr:?}");
    };
    assert_eq!("$.traits.linked_accounts", path);
    assert!(matches!(*doc, Expr::CompoundIdentifier(_)));

    // JSON_VALUE defaults to no RETURNING clause, and stores exactly the
    // three supported RETURNING types.
    for (sql, expected) in [
        ("JSON_VALUE(x, '$.a')", None),
        (
            "JSON_VALUE(x, '$.a' RETURNING VARCHAR)",
            Some(DataType::Varchar(None)),
        ),
        (
            "JSON_VALUE(x, '$.a' RETURNING BOOLEAN)",
            Some(DataType::Boolean),
        ),
        (
            "JSON_VALUE(x, '$.a' RETURNING DOUBLE PRECISION)",
            Some(DataType::DoublePrecision),
        ),
    ] {
        let expr = assert_arroyo_expr_roundtrip(sql);
        let Expr::JsonValue {
            doc,
            path,
            returning,
        } = expr
        else {
            panic!("expected Expr::JsonValue for {sql}, got {expr:?}");
        };
        assert_eq!("$.a", path);
        assert_eq!(expected, returning);
        assert!(matches!(*doc, Expr::Identifier(_)), "got doc {doc:?}");
    }
}

#[test]
fn test_json_object_roundtrip() {
    // The empty constructor
    assert_arroyo_expr_roundtrip("JSON_OBJECT()");

    // A single pair
    assert_arroyo_expr_roundtrip("JSON_OBJECT('name' VALUE 'value')");

    // Embedded single quotes in both the key and the value
    assert_arroyo_expr_roundtrip("JSON_OBJECT('o''brien' VALUE 'x''y')");

    // The exact JSON_OBJECT from the ticket SQL: mixed literal values and a
    // JSON_QUERY value with FORMAT JSON.
    assert_arroyo_expr_roundtrip(
        "JSON_OBJECT('source' VALUE 'google', 'medium' VALUE '', \
         'accounts' VALUE JSON_QUERY(e.payload, '$.traits.linked_accounts') FORMAT JSON)",
    );

    // Mixed value expressions: NULL, CAST, a nested JSON_OBJECT, a column,
    // an arithmetic expression, and JSON_QUERY FORMAT JSON.
    assert_arroyo_expr_roundtrip(
        "JSON_OBJECT('a' VALUE NULL, 'b' VALUE CAST(NULL AS VARCHAR), \
         'c' VALUE JSON_OBJECT('nested' VALUE 1), 'd' VALUE some_col, \
         'e' VALUE JSON_QUERY(payload, '$.q') FORMAT JSON, 'f' VALUE 1 + 2)",
    );
}

#[test]
fn test_json_object_ast_shape_and_pair_order() {
    let expr = assert_arroyo_expr_roundtrip(
        "JSON_OBJECT('a' VALUE 1, 'b' VALUE JSON_QUERY(x, '$.q') FORMAT JSON, 'c' VALUE NULL)",
    );
    let Expr::JsonObject { pairs } = expr else {
        panic!("expected Expr::JsonObject, got {expr:?}");
    };
    assert_eq!(3, pairs.len());
    let keys: Vec<&str> = pairs.iter().map(|pair| pair.key.as_str()).collect();
    assert_eq!(keys, ["a", "b", "c"]);
    assert!(!pairs[0].format_json);
    assert!(pairs[1].format_json);
    assert!(!pairs[2].format_json);
    assert!(
        matches!(pairs[1].value, Expr::JsonQuery { .. }),
        "expected a nested JSON_QUERY value, got {:?}",
        pairs[1].value
    );
}

#[test]
fn test_json_object_optional_key_keyword() {
    // The optional `KEY` keyword is accepted and canonicalized away by
    // Display; re-parsing the Display yields the same AST.
    let expr = parse_arroyo_expr("JSON_OBJECT(KEY 'a' VALUE 1, KEY 'b' VALUE 2)");
    let displayed = expr.to_string();
    assert_eq!("JSON_OBJECT('a' VALUE 1, 'b' VALUE 2)", displayed);
    let reparsed = parse_arroyo_expr(&displayed);
    assert_eq!(expr, reparsed);
}

#[test]
fn test_sql_json_embedded_quote_contents() {
    // Embedded single quotes are unescaped when stored in the AST and
    // re-escaped by Display, so parse -> Display -> parse is lossless.
    let expr = assert_arroyo_expr_roundtrip("JSON_VALUE(x, '$.a''b')");
    let Expr::JsonValue { path, .. } = expr else {
        panic!("expected Expr::JsonValue, got {expr:?}");
    };
    assert_eq!("$.a'b", path);

    let expr = assert_arroyo_expr_roundtrip("JSON_OBJECT('o''brien' VALUE 'x''y')");
    let Expr::JsonObject { pairs } = expr else {
        panic!("expected Expr::JsonObject, got {expr:?}");
    };
    assert_eq!("o'brien", pairs[0].key);
    match &pairs[0].value {
        Expr::Value(value) => assert_eq!(Value::SingleQuotedString("x'y".to_string()), value.value),
        other => panic!("expected a string literal value, got {other:?}"),
    }
}

#[test]
fn test_ticket_sql_roundtrip() {
    let sql = "SELECT JSON_EXISTS(e.payload, '$.traits.name'), \
               JSON_VALUE(e.payload, '$.traits.name'), \
               JSON_VALUE(e.payload, '$.traits.quality_score' RETURNING DOUBLE PRECISION), \
               JSON_VALUE(e.payload, '$.traits.steam_wishlisted' RETURNING BOOLEAN), \
               JSON_QUERY(e.payload, '$.traits.linked_accounts'), \
               JSON_OBJECT('source' VALUE 'google', 'medium' VALUE '', \
                           'accounts' VALUE JSON_QUERY(e.payload, '$.traits.linked_accounts') FORMAT JSON)";
    let statement = assert_arroyo_stmt_roundtrip(sql);

    // Sanity-check that the projection holds the dedicated variants.
    let Statement::Query(query) = &statement else {
        panic!("expected a query, got {statement:?}");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a select, got {:?}", query.body);
    };
    assert_eq!(6, select.projection.len());
    let exprs: Vec<&Expr> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr,
            other => panic!("expected an unnamed expression, got {other:?}"),
        })
        .collect();
    assert!(matches!(exprs[0], Expr::JsonExists { .. }));
    assert!(matches!(
        exprs[2],
        Expr::JsonValue {
            returning: Some(DataType::DoublePrecision),
            ..
        }
    ));
    assert!(matches!(
        exprs[3],
        Expr::JsonValue {
            returning: Some(DataType::Boolean),
            ..
        }
    ));
    assert!(matches!(exprs[4], Expr::JsonQuery { .. }));
    assert!(matches!(exprs[5], Expr::JsonObject { .. }));
}

#[test]
fn test_sql_json_on_error_empty_and_wrapper_clauses_rejected() {
    // ON ERROR / ON EMPTY
    assert_arroyo_parse_error("SELECT JSON_EXISTS(x, '$.a' ON ERROR NULL)", "ON ERROR");
    assert_arroyo_parse_error("SELECT JSON_VALUE(x, '$.a' ON EMPTY NULL)", "ON EMPTY");

    // NULL ON ERROR / NULL ON EMPTY
    assert_arroyo_parse_error("SELECT JSON_VALUE(x, '$.a' NULL ON ERROR)", "NULL ON ERROR");
    assert_arroyo_parse_error("SELECT JSON_VALUE(x, '$.a' NULL ON EMPTY)", "NULL ON EMPTY");

    // ERROR ON ... / EMPTY ON ...
    assert_arroyo_parse_error("SELECT JSON_VALUE(x, '$.a' ERROR ON NULL)", "ERROR ON NULL");
    assert_arroyo_parse_error(
        "SELECT JSON_QUERY(x, '$.a' ERROR ON DEFAULT)",
        "ERROR ON DEFAULT",
    );

    // JSON_QUERY wrapper clauses
    assert_arroyo_parse_error("SELECT JSON_QUERY(x, '$.a' ARRAY)", "ARRAY");
    assert_arroyo_parse_error("SELECT JSON_QUERY(x, '$.a' OBJECT)", "OBJECT");
    assert_arroyo_parse_error("SELECT JSON_QUERY(x, '$.a' SCALAR)", "SCALAR");
    assert_arroyo_parse_error("SELECT JSON_QUERY(x, '$.a' CONDITIONAL)", "CONDITIONAL");
    assert_arroyo_parse_error(
        "SELECT JSON_QUERY(x, '$.a' WITHOUT ARRAY WRAPPER)",
        "WITHOUT ARRAY WRAPPER",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_QUERY(x, '$.a' WITH SINGLE PAIR)",
        "WITH SINGLE PAIR",
    );

    // PASSING
    assert_arroyo_parse_error("SELECT JSON_EXISTS(x, '$.a' PASSING y AS a)", "PASSING");

    // RETURNING is not allowed on JSON_EXISTS or JSON_QUERY
    assert_arroyo_parse_error(
        "SELECT JSON_EXISTS(x, '$.a' RETURNING VARCHAR)",
        "RETURNING",
    );
    assert_arroyo_parse_error("SELECT JSON_QUERY(x, '$.a' RETURNING VARCHAR)", "RETURNING");
}

#[test]
fn test_json_value_unsupported_returning_types_rejected() {
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING INT)",
        "RETURNING type in JSON_VALUE: INT",
    );
    // The supported-type list is derived from the same table that enforces
    // it; pin the exact wording of the derived message.
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING INT)",
        "supported types are VARCHAR, BOOLEAN, and DOUBLE PRECISION",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING VARCHAR(10))",
        "VARCHAR(10)",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING TEXT)",
        "RETURNING type in JSON_VALUE: TEXT",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING DOUBLE)",
        "RETURNING type in JSON_VALUE: DOUBLE",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, '$.a' RETURNING BOOL)",
        "RETURNING type in JSON_VALUE: BOOL",
    );
}

#[test]
fn test_json_object_unsupported_clauses_rejected() {
    // ABSENT ON NULL / NULL ON NULL
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT('a' VALUE x ABSENT ON NULL)",
        "ABSENT ON NULL",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT('a' VALUE x NULL ON NULL)",
        "NULL ON NULL",
    );

    // UNIQUE / WITH UNIQUE KEYS
    assert_arroyo_parse_error("SELECT JSON_OBJECT('a' VALUE x UNIQUE)", "UNIQUE");
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT('a' VALUE x WITH UNIQUE KEYS)",
        "WITH UNIQUE KEYS",
    );

    // DEFAULT ... ON ... both after and in place of a value
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT('a' VALUE x DEFAULT y ON NULL)",
        "DEFAULT",
    );
    assert_arroyo_parse_error("SELECT JSON_OBJECT('a' VALUE DEFAULT y ON NULL)", "DEFAULT");
}

#[test]
fn test_json_object_non_literal_keys_rejected() {
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT(1 VALUE x)",
        "keys must be string literals",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT(name VALUE x)",
        "keys must be string literals",
    );
    assert_arroyo_parse_error("SELECT JSON_OBJECT(NULL ON NULL)", "NULL ON NULL");
    assert_arroyo_parse_error("SELECT JSON_OBJECT(ABSENT ON NULL)", "ABSENT ON NULL");
}

#[test]
fn test_sql_json_trailing_garbage_and_non_literal_paths_rejected() {
    // Trailing garbage between the last argument and `)`
    assert_arroyo_parse_error(
        "SELECT JSON_EXISTS(x, '$.a' trailing garbage)",
        "trailing garbage",
    );
    assert_arroyo_parse_error("SELECT JSON_VALUE(x, '$.a' junk)", "junk");
    assert_arroyo_parse_error(
        "SELECT JSON_OBJECT('a' VALUE x, 'b' VALUE y trailing)",
        "trailing",
    );

    // The path must be a single-quoted string literal
    assert_arroyo_parse_error(
        "SELECT JSON_EXISTS(x, path_col)",
        "path must be a single-quoted string literal",
    );
    assert_arroyo_parse_error(
        "SELECT JSON_VALUE(x, 42)",
        "path must be a single-quoted string literal",
    );

    // A missing closing paren still produces a precise error
    assert_arroyo_parse_error("SELECT JSON_EXISTS(x, '$.a'", "Expected: )");
}

#[test]
fn test_sql_json_functions_dialect_isolation() {
    // Arroyo parses JSON_OBJECT into the dedicated variant...
    let expr = arroyo_dialects().verified_expr("JSON_OBJECT('name' VALUE 'value')");
    let Expr::JsonObject { pairs } = expr else {
        panic!("expected Expr::JsonObject under ArroyoDialect, got {expr:?}");
    };
    assert_eq!(1, pairs.len());
    assert_eq!("name", pairs[0].key);

    // ...while Postgres keeps parsing it as a plain function (mirroring
    // tests/sqlparser_postgres.rs::test_fn_arg_with_value_operator).
    let pg = TestedDialects::new(vec![Box::new(PostgreSqlDialect {})]);
    match pg.verified_expr("JSON_OBJECT('name' VALUE 'value')") {
        Expr::Function(Function { name, .. }) => assert_eq!("JSON_OBJECT", name.to_string()),
        other => panic!("expected a generic function under PostgresDialect, got {other:?}"),
    }

    // GenericDialect keeps parsing JSON_EXISTS as a plain function too.
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    match generic.verified_expr("JSON_EXISTS(payload, '$.traits.name')") {
        Expr::Function(Function { name, .. }) => assert_eq!("JSON_EXISTS", name.to_string()),
        other => panic!("expected a generic function under GenericDialect, got {other:?}"),
    }
}

#[test]
fn test_sql_json_function_name_matching() {
    // Function names are matched case-insensitively, and Display
    // canonicalizes them to uppercase.
    let expr = parse_arroyo_expr("json_value(x, '$.a')");
    assert_eq!("JSON_VALUE(x, '$.a')", expr.to_string());
    let reparsed = parse_arroyo_expr(&expr.to_string());
    assert_eq!(expr, reparsed);

    // Quoted names are ordinary identifiers: they are not special-cased
    // and parse as generic function calls.
    let expr = assert_arroyo_expr_roundtrip("\"JSON_OBJECT\"(x, '$.a')");
    assert!(matches!(expr, Expr::Function(_)), "got {expr:?}");
}
