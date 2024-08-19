use crate::{
    ir::node::NodeId,
    ir::{transformation::helpers::sql_to_optimized_ir, ArenaType},
};
use pretty_assertions::assert_eq;

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 664] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 564
		[id: 564] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 032] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer })]
				[id: 132] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 1, col_type: String })]
				[id: 232] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 964] relation: Projection
	Children:
		Child_id = 664
	Output_id: 864
		[id: 864] expression: Row [distribution = Some(Any)]
			List:
				[id: 532] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}

#[test]
fn simple_join() {
    let query = r#"SELECT "id" FROM
                        (SELECT "id" FROM "test_space") as "t1"
                        INNER JOIN
                        (SELECT "identification_number" FROM "hash_testing") as "t2"
                        ON "t1"."id" = "t2"."identification_number""#;
    let plan = sql_to_optimized_ir(query, vec![]);
    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 664] relation: ScanRelation
	Relation: test_space
	[No children]
	Output_id: 564
		[id: 564] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 032] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 0, col_type: Unsigned })]
				[id: 132] expression: Alias [name = sysFrom, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 1, col_type: Unsigned })]
				[id: 232] expression: Alias [name = FIRST_NAME, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 2, col_type: String })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 964] relation: Projection
	Children:
		Child_id = 664
	Output_id: 864
		[id: 864] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 532] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 1264] relation: ScanSubQuery
	Alias: t1
	Children:
		Child_id = 964
	Output_id: 1164
		[id: 1164] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 632] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 12, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 1964] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 1864
		[id: 1864] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 732] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer })]
				[id: 832] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 1, col_type: String })]
				[id: 932] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean })]
				[id: 1032] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned })]
				[id: 1132] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 2264] relation: Projection
	Children:
		Child_id = 1964
	Output_id: 2164
		[id: 2164] expression: Row [distribution = Some(Any)]
			List:
				[id: 1232] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 22, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 2564] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 2264
	Output_id: 2464
		[id: 2464] expression: Row [distribution = Some(Any)]
			List:
				[id: 1332] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 25, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 2564
	Output_id: 4064
		[id: 4064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1932] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 3364] relation: InnerJoin
	Condition:
		[id: 1832] expression: Bool [op: =]
			Left child
			[id: 3764] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
				List:
					[id: 2664] expression: Reference
						Alias: id
						Referenced table name (or alias): t1
						Parent: Some(NodeId { offset: 33, arena_type: Arena64 })
						target_id: 0
						Column type: unsigned
			Right child
			[id: 3864] expression: Row [distribution = Some(Any)]
				List:
					[id: 2864] expression: Reference
						Alias: identification_number
						Referenced table name (or alias): t2
						Parent: Some(NodeId { offset: 33, arena_type: Arena64 })
						target_id: 1
						Column type: integer
	Children:
		Child_id = 1264
		Child_id = 0136
	Output_id: 3264
		[id: 3264] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [1] }, Key { positions: [0] }}) })]
			List:
				[id: 1532] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 33, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned })]
				[id: 1632] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 33, arena_type: Arena64 }), targets: Some([1]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 3664] relation: Projection
	Children:
		Child_id = 3364
	Output_id: 3564
		[id: 3564] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1732] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 36, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}

#[test]
fn simple_join_subtree() {
    let query = r#"SELECT "id" FROM
                        (SELECT "id" FROM "test_space") as "t1"
                        INNER JOIN
                        (SELECT "identification_number" FROM "hash_testing") as "t2"
                        ON "t1"."id" = "t2"."identification_number""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    // Taken from the expected arena output in the `simple_join` test.
    let inner_join_inner_child_id = NodeId {
        offset: 0,
        arena_type: ArenaType::Arena136,
    };

    let actual_arena_subtree = plan
        .formatted_arena_subtree(inner_join_inner_child_id)
        .unwrap();
    let mut expected_arena_subtree = String::new();
    expected_arena_subtree.push_str(
        r#"---------------------------------------------
[id: 1964] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 1864
		[id: 1864] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 732] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer })]
				[id: 832] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 1, col_type: String })]
				[id: 932] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean })]
				[id: 1032] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned })]
				[id: 1132] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 19, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 2264] relation: Projection
	Children:
		Child_id = 1964
	Output_id: 2164
		[id: 2164] expression: Row [distribution = Some(Any)]
			List:
				[id: 1232] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 22, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 2564] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 2264
	Output_id: 2464
		[id: 2464] expression: Row [distribution = Some(Any)]
			List:
				[id: 1332] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 25, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 2564
	Output_id: 4064
		[id: 4064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1932] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: Integer })]
---------------------------------------------
"#
    );

    assert_eq!(expected_arena_subtree, actual_arena_subtree);
}

#[test]
fn simple_aggregation_with_group_by() {
    let query = r#"SELECT "product_code" FROM "hash_testing" GROUP BY "product_code""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();
    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 664] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 564
		[id: 564] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 032] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer })]
				[id: 132] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 1, col_type: String })]
				[id: 232] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 6, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 1464] relation: GroupBy [is_final = false]
	Gr_cols:
		Gr_col: Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String })
	Children:
		Child_id = 664
	Output_id: 1364
		[id: 1364] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 532] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer })]
				[id: 632] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String })]
				[id: 732] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 2, col_type: Boolean })]
				[id: 832] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 3, col_type: Unsigned })]
				[id: 932] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 4, col_type: Unsigned })]
---------------------------------------------
---------------------------------------------
[id: 1964] relation: Projection
	Children:
		Child_id = 1464
	Output_id: 1864
		[id: 1864] expression: Row [distribution = Some(Any)]
			List:
				[id: 1132] expression: Alias [name = column_764, child = Reference(Reference { parent: Some(NodeId { offset: 14, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String })]
---------------------------------------------
---------------------------------------------
[id: 2264] relation: ScanSubQuery
	Children:
		Child_id = 1964
	Output_id: 2164
		[id: 2164] expression: Row [distribution = Some(Any)]
			List:
				[id: 1232] expression: Alias [name = column_764, child = Reference(Reference { parent: Some(NodeId { offset: 22, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 2264
	Output_id: 2964
		[id: 2964] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1432] expression: Alias [name = column_764, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: String })]
---------------------------------------------
---------------------------------------------
[id: 2664] relation: GroupBy [is_final = true]
	Gr_cols:
		Gr_col: Reference(Reference { parent: Some(NodeId { offset: 26, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String })
	Children:
		Child_id = 0136
	Output_id: 2564
		[id: 2564] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1332] expression: Alias [name = column_764, child = Reference(Reference { parent: Some(NodeId { offset: 26, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String })]
---------------------------------------------
---------------------------------------------
[id: 1764] relation: Projection
	Children:
		Child_id = 2664
	Output_id: 1664
		[id: 1664] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1032] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 17, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}
