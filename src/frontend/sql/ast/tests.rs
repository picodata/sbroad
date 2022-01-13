use super::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;
use traversal::DftPost;

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
}

#[test]
fn transfrom_select_2() {
    let query = r#"select a from t"#;
    let mut ast = AbstractSyntaxTree::new(query).unwrap();
    ast.transform_select().unwrap();
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
fn transfrom_select_3() {
    let query = r#"select a from t where a = 1"#;
    let mut ast = AbstractSyntaxTree::new(query).unwrap();
    ast.transform_select().unwrap();
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
fn transfrom_select_4() {
    let query = r#"select * from t1 inner join t2 on t1.a = t2.a"#;
    let mut ast = AbstractSyntaxTree::new(query).unwrap();
    ast.transform_select().unwrap();
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
fn transfrom_select_5() {
    let query = r#"select * from t1 inner join t2 on t1.a = t2.a where t1.a > 0"#;
    let mut ast = AbstractSyntaxTree::new(query).unwrap();
    ast.transform_select().unwrap();
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
    let mut ast = AbstractSyntaxTree::new(query).unwrap();
    ast.transform_select().unwrap();
    let top = ast.top.unwrap();
    let mut dft_pre = DftPost::new(&top, |node| ast.nodes.tree_iter(node));

    let (_, table_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*table_id).unwrap();
    assert_eq!(node.rule, Type::Table);

    let (_, scan_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*scan_id).unwrap();
    assert_eq!(node.rule, Type::Scan);

    let (_, a_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*a_id).unwrap();
    assert_eq!(node.rule, Type::String);

    let (_, num_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*num_id).unwrap();
    assert_eq!(node.rule, Type::Number);

    let (_, eq_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*eq_id).unwrap();
    assert_eq!(node.rule, Type::Eq);

    let (_, selection_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*selection_id).unwrap();
    assert_eq!(node.rule, Type::Selection);

    let (_, str_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*str_id).unwrap();
    assert_eq!(node.rule, Type::String);

    let (_, col_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*col_id).unwrap();
    assert_eq!(node.rule, Type::Column);

    let (_, projection_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*projection_id).unwrap();
    assert_eq!(node.rule, Type::Projection);

    // This node can be skipped during AST -> IR
    let (_, select_id) = dft_pre.next().unwrap();
    let node = ast.nodes.get_node(*select_id).unwrap();
    assert_eq!(node.rule, Type::Select);

    assert_eq!(None, dft_pre.next());
}
