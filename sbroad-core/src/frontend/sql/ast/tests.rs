use super::*;
use crate::frontend::Ast;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn ast() {
    let query = r#"SELECT "identification_number", "product_code" FROM "test_space" WHERE "identification_number" = 1;"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("simple_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected: AbstractSyntaxTree = AbstractSyntaxTree::from_yaml(&s).unwrap();
    assert_eq!(expected, ast);

    let empty_yaml = "";
    assert_eq!(
        SbroadError::FailedTo(Action::Serialize, Some(Entity::AST), "EndOfStream".into(),),
        AbstractSyntaxTree::from_yaml(empty_yaml).unwrap_err()
    );
}

#[test]
fn transform_select_2() {
    let query = r#"select a from t"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("transform_select_2.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected: AbstractSyntaxTree = AbstractSyntaxTree::from_yaml(&s).unwrap();
    assert_eq!(expected, ast);
}

#[test]
fn transform_select_3() {
    let query = r#"select a from t where a = 1"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("transform_select_3.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected: AbstractSyntaxTree = AbstractSyntaxTree::from_yaml(&s).unwrap();
    assert_eq!(expected, ast);
}

#[test]
fn transform_select_4() {
    let query = r#"select * from t1 inner join t2 on t1.a = t2.a"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("transform_select_4.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected: AbstractSyntaxTree = AbstractSyntaxTree::from_yaml(&s).unwrap();
    assert_eq!(expected, ast);
}

#[test]
fn transform_select_5() {
    let query = r#"select * from t1 inner join t2 on t1.a = t2.a where t1.a > 0"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("transform_select_5.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected: AbstractSyntaxTree = AbstractSyntaxTree::from_yaml(&s).unwrap();
    assert_eq!(expected, ast);
}

#[test]
fn traversal() {
    let query = r#"select a from t where a = 1"#;
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let top = ast.top.unwrap();
    let mut dft_post = PostOrder::with_capacity(|node| ast.nodes.ast_iter(node), 64);
    let mut iter = dft_post.iter(top);

    let (_, table_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(table_id).unwrap();
    assert_eq!(node.rule, Type::Table);

    let (_, scan_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(scan_id).unwrap();
    assert_eq!(node.rule, Type::Scan);

    let (_, sel_name_a_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(sel_name_a_id).unwrap();
    assert_eq!(node.rule, Type::ColumnName);

    let (_, a_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(a_id).unwrap();
    assert_eq!(node.rule, Type::Reference);

    let (_, num_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(num_id).unwrap();
    assert_eq!(node.rule, Type::Unsigned);

    let (_, eq_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(eq_id).unwrap();
    assert_eq!(node.rule, Type::Eq);

    let (_, selection_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(selection_id).unwrap();
    assert_eq!(node.rule, Type::Selection);

    let (_, prj_name_a_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(prj_name_a_id).unwrap();
    assert_eq!(node.rule, Type::ColumnName);

    let (_, str_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(str_id).unwrap();
    assert_eq!(node.rule, Type::Reference);

    let (_, alias_name_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(alias_name_id).unwrap();
    assert_eq!(node.rule, Type::AliasName);

    let (_, alias_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(alias_id).unwrap();
    assert_eq!(node.rule, Type::Alias);

    let (_, col_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(col_id).unwrap();
    assert_eq!(node.rule, Type::Column);

    let (_, projection_id) = iter.next().unwrap();
    let node = ast.nodes.get_node(projection_id).unwrap();
    assert_eq!(node.rule, Type::Projection);

    assert_eq!(None, iter.next());
}

#[test]
fn invalid_query() {
    let query = r#"select a frAm t"#;
    let ast = AbstractSyntaxTree::new(query).unwrap_err();
    assert_eq!(
        format!(
            r#"rule parsing error:  --> 1:8
  |
1 | select a frAm t
  |        ^---
  |
  = expected Alias, Asterisk, Function, Cast, Concat, True, False, Null, Decimal, Double, Integer, Unsigned, Row, or Parameter"#,
        ),
        format!("{ast}"),
    );
}

#[test]
fn invalid_condition() {
    let query = r#"SELECT "identification_number", "product_code" FROM "test_space" WHERE
    "identification_number" = 1 "product_code" = 2"#;
    let ast = AbstractSyntaxTree::new(query).unwrap_err();
    assert_eq!(
        format!(
            r#"rule parsing error:  --> 2:33
  |
2 |     "identification_number" = 1 "product_code" = 2
  |                                 ^---
  |
  = expected EOI"#,
        ),
        format!("{ast}"),
    )
}
