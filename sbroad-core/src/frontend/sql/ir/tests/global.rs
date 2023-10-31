use crate::errors::SbroadError;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::distribution::Distribution;
use crate::ir::operator::Relational;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::{FilterFn, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::{Node, Plan};
use pretty_assertions::assert_eq;

#[test]
fn front_sql_check_global_tbl_support() {
    macro_rules! global_tbl_err {
        ($e:expr) => {
            concat!($e, " is not supported for global tables")
        };
    }

    let metadata = &RouterConfigurationMock::new();

    check_error(
        r#"select * from "global_t" join (select "a" as "oa" from "t3") on true"#,
        metadata,
        global_tbl_err!("Join"),
    );
    check_error(
        r#"select "a" from "global_t" union all select * from (select "a" as "oa" from "t3")"#,
        metadata,
        global_tbl_err!("UnionAll"),
    );
    check_error(
        r#"select "a" from "global_t" except select * from (select "a" as "oa" from "t3")"#,
        metadata,
        global_tbl_err!("Except"),
    );
    check_error(
        r#"select sum("a") from "global_t""#,
        metadata,
        global_tbl_err!("Aggregate"),
    );
    check_error(
        r#"select "a" from "global_t" group by "a""#,
        metadata,
        global_tbl_err!("GroupBy"),
    );
    check_error(
        r#"insert into "global_t" values (1, 1)"#,
        metadata,
        "expected sharded table",
    );
    check_error(
        r#"insert into "global_t" values (1, 1)"#,
        metadata,
        "expected sharded table",
    );
    check_error(
        r#"delete from "global_t""#,
        metadata,
        global_tbl_err!("Delete"),
    );
    check_error(
        r#"update "t3" set "b" = "b1" from (select "b" as "b1" from "global_t")"#,
        metadata,
        global_tbl_err!("Join"),
    );
    check_error(
        r#"update "global_t" set "b" = 1"#,
        metadata,
        "expected sharded table",
    );

    fn check_error(input: &str, metadata: &RouterConfigurationMock, expected_err: &str) {
        let res = build(input, metadata);
        if res.is_ok() {}
        let err = res.unwrap_err();

        assert_eq!(true, err.to_string().contains(expected_err));

        fn build(input: &str, metadata: &RouterConfigurationMock) -> Result<(), SbroadError> {
            let ast = AbstractSyntaxTree::new(input)?;
            ast.resolve_metadata(metadata)?.optimize()
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum DistMock {
    Segment,
    Global,
    Single,
    Any,
}

impl From<&Distribution> for DistMock {
    fn from(dist: &Distribution) -> Self {
        match dist {
            Distribution::Any => DistMock::Any,
            Distribution::Global => DistMock::Global,
            Distribution::Segment { .. } => DistMock::Segment,
            Distribution::Single => DistMock::Single,
        }
    }
}

fn collect_relational<'filter>(plan: &Plan, predicate: FilterFn<'filter>) -> Vec<(usize, usize)> {
    let mut rel_tree = PostOrderWithFilter::with_capacity(
        |node| plan.nodes.rel_iter(node),
        REL_CAPACITY,
        predicate,
    );
    rel_tree.populate_nodes(plan.get_top().unwrap());
    let nodes = rel_tree.take_nodes();
    drop(rel_tree);
    nodes
}

fn check_distributions(plan: &Plan, nodes: &[(usize, usize)], expected_distributions: &[DistMock]) {
    assert_eq!(
        expected_distributions.len(),
        nodes.len(),
        "different number of nodes"
    );
    for ((level, id), expected) in nodes.iter().zip(expected_distributions.iter()) {
        let actual: DistMock = plan.get_rel_distribution(*id).unwrap().into();
        assert_eq!(
            expected, &actual,
            "wrong distribution for node ({id}) at level {level}"
        );
    }
}

fn check_selection_dist(plan: &Plan, expected_dist: DistMock) {
    let filter = |id: usize| -> bool {
        matches!(
            plan.get_node(id),
            Ok(Node::Relational(Relational::Selection { .. }))
        )
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, &[expected_dist]);
}

#[test]
fn front_sql_global_tbl_sq1() {
    // sq has distribution Any, motion(full) must be inserted,
    // for sq with single distribution motion is not needed,
    // it will calculated on the router
    let input = r#"
    select * from "global_t"
    where "a" in (select "a" as a1 from "t") or
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
    selection (ROW("global_t"."a"::integer) in ROW($0) or ROW("global_t"."a"::integer) in ROW($1))
        scan "global_t"
subquery $0:
motion [policy: full]
            scan
                projection ("t"."a"::unsigned -> "A1")
                    scan "t"
subquery $1:
scan
            projection (sum(("sum_43"::decimal))::decimal -> "COL_1")
                motion [policy: full]
                    scan
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_43")
                            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_multiple_sqs1() {
    // For sq with single distribution we never need
    // a motion. But now the first subquery has
    // Segment distribution, and thus does need a motion.
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t") and
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
    selection ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0) and ROW("global_t"."a"::integer) in ROW($1)
        scan "global_t"
subquery $0:
scan
            projection ("t"."a"::unsigned -> "A1", "t"."b"::unsigned -> "B1")
                scan "t"
subquery $1:
scan
            projection (sum(("sum_48"::decimal))::decimal -> "COL_1")
                motion [policy: full]
                    scan
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_48")
                            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_selection_dist(&plan, DistMock::Any);
}

#[test]
fn front_sql_global_tbl_multiple_sqs2() {
    // For sq with single distribution we never need
    // a motion. But now the first subquery has
    // Segment distribution, and thus does need a motion,
    // but it is connected with the second sq via `OR`,
    // so the motion is needed for the first subquery.
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t") or
    "a" in (select sum("a") from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
    selection (ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0) or ROW("global_t"."a"::integer) in ROW($1))
        scan "global_t"
subquery $0:
motion [policy: full]
            scan
                projection ("t"."a"::unsigned -> "A1", "t"."b"::unsigned -> "B1")
                    scan "t"
subquery $1:
scan
            projection (sum(("sum_48"::decimal))::decimal -> "COL_1")
                motion [policy: full]
                    scan
                        projection (sum(("t"."a"::unsigned))::decimal -> "sum_48")
                            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_sq2() {
    // sq has distribution Segment, no motion must be inserted
    let input = r#"
    select * from "global_t"
    where ("a", "b") in (select "a" as a1, "b" as b1 from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
    selection ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0)
        scan "global_t"
subquery $0:
scan
            projection ("t"."a"::unsigned -> "A1", "t"."b"::unsigned -> "B1")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_selection_dist(&plan, DistMock::Any);
}

#[test]
fn front_sql_global_tbl_sq3() {
    // sq has distribution Segment, but due to `not in` motion(full) must be inserted
    let input = r#"
    select * from "global_t"
    where ("a", "b") not in (select "a" as a1, "b" as b1 from "t") or
    ("a", "b") < (select "a" as a1, "b" as b1 from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
    selection (not ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($1, $1) or ROW("global_t"."a"::integer, "global_t"."b"::integer) < ROW($0, $0))
        scan "global_t"
subquery $0:
motion [policy: full]
            scan
                projection ("t"."a"::unsigned -> "A1", "t"."b"::unsigned -> "B1")
                    scan "t"
subquery $1:
motion [policy: full]
            scan
                projection ("t"."a"::unsigned -> "A1", "t"."b"::unsigned -> "B1")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_selection_dist(&plan, DistMock::Global);
}

#[test]
fn front_sql_global_tbl_sq4() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "product_code" from "t" inner join "hash_testing"
    on "t"."a" = "hash_testing"."identification_number" and "hash_testing"."product_code"
    in (select "a" as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("hash_testing"."product_code"::string -> "product_code")
    join on ROW("t"."a"::unsigned) = ROW("hash_testing"."identification_number"::integer) and ROW("hash_testing"."product_code"::string) in ROW($0)
        scan "t"
            projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                scan "t"
        motion [policy: full]
            scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                    scan "hash_testing"
subquery $0:
scan
            projection ("global_t"."a"::integer -> "A1")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_tbl_sq5() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") AND
    "t"."c" in (select "a" as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
    join on ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned) and ROW("t"."c"::unsigned) in ROW($0)
        scan "t"
            projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                scan "t"
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                scan "t2"
subquery $0:
scan
            projection ("global_t"."a"::integer -> "A1")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_tbl_sq6() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") OR
    "t"."c" in (select "a" as a1 from "global_t") OR
    "t"."d" not in (select "a" as a1 from "global_t") AND
    exists (select "a" * 20 as a1 from "global_t" where "a" = 1)
    where "e" in (select "a" * 10 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
    selection ROW("t2"."e"::unsigned) in ROW($3)
        join on ((ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned) or ROW("t"."c"::unsigned) in ROW($0)) or exists ROW($2) and not ROW("t"."d"::unsigned) in ROW($1))
            scan "t"
                projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                    scan "t"
            motion [policy: full]
                scan "t2"
                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                        scan "t2"
subquery $0:
scan
                projection ("global_t"."a"::integer -> "A1")
                    scan "global_t"
subquery $1:
scan
                projection ("global_t"."a"::integer -> "A1")
                    scan "global_t"
subquery $2:
scan
                projection (ROW("global_t"."a"::integer) * ROW(20::unsigned) -> "A1")
                    selection ROW("global_t"."a"::integer) = ROW(1::unsigned)
                        scan "global_t"
subquery $3:
scan
            projection (ROW("global_t"."a"::integer) * ROW(10::unsigned) -> "COL_1")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_tbl_sq7() {
    // Reading from global subquery should not produce motion
    let input = r#"
    select "a", "f" from "t" inner join "t2"
    on ("t"."a", "t"."b") = ("t2"."e", "t2"."f") OR
    "t"."c" in (select "a" as a1 from "global_t") OR
    "t"."d" not in (select "a" as a1 from "global_t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t2"."f"::unsigned -> "f")
    join on ((ROW("t"."a"::unsigned, "t"."b"::unsigned) = ROW("t2"."e"::unsigned, "t2"."f"::unsigned) or ROW("t"."c"::unsigned) in ROW($0)) or not ROW("t"."d"::unsigned) in ROW($1))
        scan "t"
            projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                scan "t"
        motion [policy: full]
            scan "t2"
                projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    scan "t2"
subquery $0:
scan
            projection ("global_t"."a"::integer -> "A1")
                scan "global_t"
subquery $1:
scan
            projection ("global_t"."a"::integer -> "A1")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
