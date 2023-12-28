use crate::errors::SbroadError;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::distribution::Distribution;
use crate::ir::operator::Relational;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::tree::traversal::{FilterFn, PostOrderWithFilter, REL_CAPACITY};
use crate::ir::value::Value;
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
        r#"update "global_t" set "b" = 1"#,
        metadata,
        "expected sharded table",
    );

    fn check_error(input: &str, metadata: &RouterConfigurationMock, expected_err: &str) {
        let res = build(input, metadata);
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

fn check_join_dist(plan: &Plan, expected_distributions: &[DistMock]) {
    let filter = |id: usize| -> bool {
        matches!(
            plan.get_node(id),
            Ok(Node::Relational(Relational::Join { .. }))
        )
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, expected_distributions);
}

#[test]
fn front_sql_global_join1() {
    let input = r#"
    select "e", "a" from "global_t"
    inner join "t2"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
    join on true::boolean
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
    check_join_dist(&plan, &[DistMock::Segment]);
}

#[test]
fn front_sql_global_join2() {
    let input = r#"
    select "e", "a" from "t2"
    inner join "global_t"
    on "e" = "a" or "b" = "f"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Segment]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
    join on (ROW("t2"."e"::unsigned) = ROW("global_t"."a"::integer) or ROW("global_t"."b"::integer) = ROW("t2"."f"::unsigned))
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                scan "t2"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join3() {
    let input = r#"
    select "e", "a" from "t2"
    left join "global_t"
    on "e" = "a" or "b" = "f"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Segment]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "global_t"."a"::integer -> "a")
    left join on (ROW("t2"."e"::unsigned) = ROW("global_t"."a"::integer) or ROW("global_t"."b"::integer) = ROW("t2"."f"::unsigned))
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                scan "t2"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join4() {
    let input = r#"
    select e from (select sum("e") as e from "t2") as s
    left join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Single]);

    let expected_explain = String::from(
        r#"projection ("S"."E"::decimal -> "E")
    left join on true::boolean
        scan "S"
            projection (sum(("sum_13"::decimal))::decimal -> "E")
                motion [policy: full]
                    scan
                        projection (sum(("t2"."e"::unsigned))::decimal -> "sum_13")
                            scan "t2"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join5() {
    let input = r#"
    select e from "global_t"
    left join (select sum("e") as e from "t2") as s
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Single]);

    let expected_explain = String::from(
        r#"projection ("S"."E"::decimal -> "E")
    left join on true::boolean
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
        scan "S"
            projection (sum(("sum_19"::decimal))::decimal -> "E")
                motion [policy: full]
                    scan
                        projection (sum(("t2"."e"::unsigned))::decimal -> "sum_19")
                            scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join6() {
    let input = r#"
    select e from "global_t"
    inner join (select "e"*"e" as e from "t2") as s
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    let expected_explain = String::from(
        r#"projection ("S"."E"::unsigned -> "E")
    join on true::boolean
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
        scan "S"
            projection (ROW("t2"."e"::unsigned) * ROW("t2"."e"::unsigned) -> "E")
                scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join7() {
    let input = r#"
    select e from (select "e"*"e" as e from "t2") as s
    inner join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    let expected_explain = String::from(
        r#"projection ("S"."E"::unsigned -> "E")
    join on true::boolean
        scan "S"
            projection (ROW("t2"."e"::unsigned) * ROW("t2"."e"::unsigned) -> "E")
                scan "t2"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join8() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Global]);

    let expected_explain = String::from(
        r#"projection ("E"::integer -> "E")
    join on true::boolean
        scan
            projection (ROW("global_t"."a"::integer) * ROW("global_t"."a"::integer) -> "E")
                scan "global_t"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join9() {
    let input = r#"
    select e from (select "e"*"e" as e from "t2")
    left join "global_t"
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    let expected_explain = String::from(
        r#"projection ("E"::unsigned -> "E")
    left join on true::boolean
        scan
            projection (ROW("t2"."e"::unsigned) * ROW("t2"."e"::unsigned) -> "E")
                scan "t2"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join10() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on e in (select "e" from "t2")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Global]);

    let expected_explain = String::from(
        r#"projection ("E"::integer -> "E")
    join on ROW("E"::integer) in ROW($0)
        scan
            projection (ROW("global_t"."a"::integer) * ROW("global_t"."a"::integer) -> "E")
                scan "global_t"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
subquery $0:
motion [policy: full]
            scan
                projection ("t2"."e"::unsigned -> "e")
                    scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_join11() {
    let input = r#"
    select e from (select "a"*"a" as e from "global_t")
    inner join "global_t"
    on (e, e) in (select "e", "f" from "t2")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    check_join_dist(&plan, &[DistMock::Any]);

    let expected_explain = String::from(
        r#"projection ("E"::integer -> "E")
    join on ROW("E"::integer, "E"::integer) in ROW($0, $0)
        scan
            projection (ROW("global_t"."a"::integer) * ROW("global_t"."a"::integer) -> "E")
                scan "global_t"
        scan "global_t"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                scan "global_t"
subquery $0:
scan
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_aggregate1() {
    let input = r#"
    select sum("a") + avg("b" + "b") from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW(sum(("global_t"."a"::integer))::decimal) + ROW(avg((ROW("global_t"."b"::integer) + ROW("global_t"."b"::integer)))::decimal) -> "COL_1")
    scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_aggregate2() {
    let input = r#"
    select sum("a") + avg("b" + "b") from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW(sum(("global_t"."a"::integer))::decimal) + ROW(avg((ROW("global_t"."b"::integer) + ROW("global_t"."b"::integer)))::decimal) -> "COL_1")
    scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_aggregate3() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    group by "b"+"a"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer) -> "COL_1", sum(("global_t"."a"::integer))::decimal -> "COL_2")
    group by (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer)) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_aggregate4() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    group by "b"+"a"
    having avg("b") > 3
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer) -> "COL_1", sum(("global_t"."a"::integer))::decimal -> "COL_2")
    having ROW(avg(("global_t"."b"::integer))::decimal) > ROW(3::unsigned)
        group by (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer)) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
            scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_aggregate5() {
    let input = r#"
    select "b"+"a", sum("a") from "global_t"
    where ("a", "b") in (select "e", "f" from "t2")
    group by "b"+"a"
    having avg("b") > 3
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_44"::integer -> "COL_1", sum(("sum_70"::decimal))::decimal -> "COL_2")
    having ROW((sum(("sum_52"::decimal::double))::decimal / sum(("count_52"::integer::double))::decimal)) > ROW(3::unsigned)
        group by ("column_44"::integer) output: ("column_44"::integer -> "column_44", "sum_52"::decimal -> "sum_52", "sum_70"::decimal -> "sum_70", "count_52"::integer -> "count_52")
            motion [policy: segment([ref("column_44")])]
                scan
                    projection (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer) -> "column_44", sum(("global_t"."b"::integer))::decimal -> "sum_52", sum(("global_t"."a"::integer))::decimal -> "sum_70", count(("global_t"."b"::integer))::integer -> "count_52")
                        group by (ROW("global_t"."b"::integer) + ROW("global_t"."a"::integer)) output: ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                            selection ROW("global_t"."a"::integer, "global_t"."b"::integer) in ROW($0, $0)
                                scan "global_t"
subquery $0:
scan
                                    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                                        scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_left_join1() {
    let input = r#"
    select "e", "b" from "global_t"
    left join "t2" on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("e"::unsigned -> "e", "b"::integer -> "b")
    motion [policy: full]
        scan
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                join on true::boolean
                    motion [policy: full]
                        scan "global_t"
                            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                scan "global_t"
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_left_join2() {
    let input = r#"
    select "e", sum("b") from "global_t"
    left join "t2" on true
    group by "e"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("e"::unsigned -> "e", sum(("b"::integer))::decimal -> "COL_1")
    group by ("e"::unsigned) output: ("a"::integer -> "a", "b"::integer -> "b", "e"::unsigned -> "e", "f"::unsigned -> "f", "g"::unsigned -> "g", "h"::unsigned -> "h")
        motion [policy: full]
            scan
                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                    join on true::boolean
                        motion [policy: full]
                            scan "global_t"
                                projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                    scan "global_t"
                        scan "t2"
                            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                                scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_left_join3() {
    let input = r#"
    select "e", "b" from 
    (select "b" * "b" as "b" from "global_t")
    left join "t2" on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("e"::unsigned -> "e", "b"::integer -> "b")
    motion [policy: full]
        scan
            projection ("b"::integer -> "b", "t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                join on true::boolean
                    motion [policy: full]
                        scan
                            projection (ROW("global_t"."b"::integer) * ROW("global_t"."b"::integer) -> "b")
                                scan "global_t"
                    scan "t2"
                        projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
                            scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_global_left_join4() {
    let input = r#"
    select "e", "b" from 
    (select "b" * "b" as "b" from "global_t")
    left join 
    (select "e" + 1 as "e" from "t2")
    on true
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("e"::unsigned -> "e", "b"::integer -> "b")
    motion [policy: full]
        scan
            projection ("b"::integer -> "b", "e"::unsigned -> "e")
                join on true::boolean
                    motion [policy: full]
                        scan
                            projection (ROW("global_t"."b"::integer) * ROW("global_t"."b"::integer) -> "b")
                                scan "global_t"
                    scan
                        projection (ROW("t2"."e"::unsigned) + ROW(1::unsigned) -> "e")
                            scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

fn check_union_dist(plan: &Plan, expected_distributions: &[DistMock]) {
    let filter = |id: usize| -> bool {
        matches!(
            plan.get_node(id),
            Ok(Node::Relational(Relational::UnionAll { .. }))
        )
    };
    let nodes = collect_relational(plan, Box::new(filter));
    check_distributions(plan, &nodes, expected_distributions);
}

#[test]
fn front_sql_global_union_all1() {
    let input = r#"
    select "a", "b" from "global_t"
    union all
    select "e", "f" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: local]
        projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
            scan "global_t"
    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
        scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    check_union_dist(&plan, &[DistMock::Any]);
}

#[test]
fn front_sql_global_union_all2() {
    let input = r#"
    select "a" from "global_t"
    union all
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: local]
        projection ("global_t"."a"::integer -> "a")
            scan "global_t"
    projection ("t2"."e"::unsigned -> "e")
        scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    check_union_dist(&plan, &[DistMock::Any]);
}

#[test]
fn front_sql_global_union_all3() {
    let input = r#"
    select * from (select "a" from "global_t"
    union all
    select sum("e") from "t2")
    union all
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("a"::integer -> "a")
        scan
            union all
                projection ("global_t"."a"::integer -> "a")
                    scan "global_t"
                motion [policy: segment([ref("COL_1")])]
                    projection (sum(("sum_23"::decimal))::decimal -> "COL_1")
                        motion [policy: full]
                            scan
                                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_23")
                                    scan "t2"
    motion [policy: local]
        projection ("global_t"."b"::integer -> "b")
            scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    check_union_dist(&plan, &[DistMock::Any, DistMock::Any]);
}

#[test]
fn front_sql_global_union_all5() {
    let input = r#"
    select "a" from "global_t"
    union all
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
    projection ("global_t"."b"::integer -> "b")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    check_union_dist(&plan, &[DistMock::Global]);
}

#[test]
fn check_plan_except_global_vs_segment() {
    let input = r#"
    select "a", "b" from "global_t"
    where "a" = ?
    except
    select "e", "f" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1)]);

    // TODO: the subtree for left except child is reused
    // from another motion, show this in explain
    let expected_explain = String::from(
        r#"except
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        selection ROW("global_t"."a"::integer) = ROW(1::unsigned)
            scan "global_t"
    motion [policy: full]
        intersect
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
                scan "t2"
            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                selection ROW("global_t"."a"::integer) = ROW(1::unsigned)
                    scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_global_vs_any() {
    let input = r#"
    select "a" from "global_t"
    except
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // TODO: the subtree for left except child is reused
    // from another motion, show this in explain
    let expected_explain = String::from(
        r#"except
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
    motion [policy: full]
        intersect
            projection ("t2"."e"::unsigned -> "e")
                scan "t2"
            projection ("global_t"."a"::integer -> "a")
                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_global_vs_global() {
    let input = r#"
    select "a" from "global_t"
    except
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
    projection ("global_t"."b"::integer -> "b")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_global_vs_single() {
    let input = r#"
    select "a" from "global_t"
    except
    select sum("e") from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
    projection (sum(("sum_23"::decimal))::decimal -> "COL_1")
        motion [policy: full]
            scan
                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_23")
                    scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_single_vs_global() {
    let input = r#"
    select sum("e") from "t2"
    except
    select "a" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection (sum(("sum_13"::decimal))::decimal -> "COL_1")
        motion [policy: full]
            scan
                projection (sum(("t2"."e"::unsigned))::decimal -> "sum_13")
                    scan "t2"
    projection ("global_t"."a"::integer -> "a")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_segment_vs_global() {
    let input = r#"
    select "e", "f" from "t2"
    except
    select "a", "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f")
        scan "t2"
    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_any_vs_global() {
    let input = r#"
    select "e" from "t2"
    except
    select "b" from "global_t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t2"."e"::unsigned -> "e")
        scan "t2"
    projection ("global_t"."b"::integer -> "b")
        scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn check_plan_except_non_trivial_global_subtree_vs_any() {
    // check that plan is correctly built when left global
    // subtree is something more difficult than a scan of
    // a global table
    let input = r#"
    select "b" from "global_t"
    left join (select "b" as b from "global_t")
    on "a" = b
    where "a" = 1
    except
    select "e" from "t2"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("global_t"."b"::integer -> "b")
        selection ROW("global_t"."a"::integer) = ROW(1::unsigned)
            left join on ROW("global_t"."a"::integer) = ROW("B"::integer)
                scan "global_t"
                    projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                        scan "global_t"
                scan
                    projection ("global_t"."b"::integer -> "B")
                        scan "global_t"
    motion [policy: full]
        intersect
            projection ("t2"."e"::unsigned -> "e")
                scan "t2"
            projection ("global_t"."b"::integer -> "b")
                selection ROW("global_t"."a"::integer) = ROW(1::unsigned)
                    left join on ROW("global_t"."a"::integer) = ROW("B"::integer)
                        scan "global_t"
                            projection ("global_t"."a"::integer -> "a", "global_t"."b"::integer -> "b")
                                scan "global_t"
                        scan
                            projection ("global_t"."b"::integer -> "B")
                                scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}