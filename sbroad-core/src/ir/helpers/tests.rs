use crate::ir::{expression::NodeId, transformation::helpers::sql_to_optimized_ir, ArenaType};
use pretty_assertions::assert_eq;

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 11] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 1] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 0, col_type: Integer }]
				[id: 3] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 1, col_type: String }]
				[id: 5] expression: Alias [name = product_units, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 2, col_type: Boolean }]
				[id: 7] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 3, col_type: Unsigned }]
				[id: 9] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 15] relation: Projection
	Children:
		Child_id = 11
	Output_id: 14
		[id: 14] expression: Row [distribution = Some(Any)]
			List:
				[id: 13] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 15, arena_type: Default }), targets: Some([0]), position: 1, col_type: String }]
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
[id: 11] relation: ScanRelation
	Relation: test_space
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1] expression: Alias [name = id, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 0, col_type: Unsigned }]
				[id: 3] expression: Alias [name = sysFrom, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 1, col_type: Unsigned }]
				[id: 5] expression: Alias [name = FIRST_NAME, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 2, col_type: String }]
				[id: 7] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 3, col_type: Unsigned }]
				[id: 9] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 15] relation: Projection
	Children:
		Child_id = 11
	Output_id: 14
		[id: 14] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 13] expression: Alias [name = id, child = Reference { parent: Some(NodeId { offset: 15, arena_type: Default }), targets: Some([0]), position: 0, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 19] relation: ScanSubQuery
	Alias: t1
	Children:
		Child_id = 15
	Output_id: 18
		[id: 18] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 17] expression: Alias [name = id, child = Reference { parent: Some(NodeId { offset: 19, arena_type: Default }), targets: Some([0]), position: 0, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 31] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 30
		[id: 30] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 21] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 0, col_type: Integer }]
				[id: 23] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 1, col_type: String }]
				[id: 25] expression: Alias [name = product_units, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 2, col_type: Boolean }]
				[id: 27] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 3, col_type: Unsigned }]
				[id: 29] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 35] relation: Projection
	Children:
		Child_id = 31
	Output_id: 34
		[id: 34] expression: Row [distribution = Some(Any)]
			List:
				[id: 33] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 35, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 39] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 35
	Output_id: 38
		[id: 38] expression: Row [distribution = Some(Any)]
			List:
				[id: 37] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 39, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 61] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 39
	Output_id: 60
		[id: 60] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 59] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 61, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 50] relation: InnerJoin
	Condition:
		[id: 57] expression: Bool [op: =]
			Left child
			[id: 55] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
				List:
					[id: 40] expression: Reference
						Alias: id
						Referenced table name (or alias): t1
						Parent: Some(NodeId { offset: 50, arena_type: Default })
						target_id: 0
						Column type: unsigned
			Right child
			[id: 56] expression: Row [distribution = Some(Any)]
				List:
					[id: 42] expression: Reference
						Alias: identification_number
						Referenced table name (or alias): t2
						Parent: Some(NodeId { offset: 50, arena_type: Default })
						target_id: 1
						Column type: integer
	Children:
		Child_id = 19
		Child_id = 61
	Output_id: 49
		[id: 49] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [1] }, Key { positions: [0] }}) })]
			List:
				[id: 46] expression: Alias [name = id, child = Reference { parent: Some(NodeId { offset: 50, arena_type: Default }), targets: Some([0]), position: 0, col_type: Unsigned }]
				[id: 48] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 50, arena_type: Default }), targets: Some([1]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 54] relation: Projection
	Children:
		Child_id = 50
	Output_id: 53
		[id: 53] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 52] expression: Alias [name = id, child = Reference { parent: Some(NodeId { offset: 54, arena_type: Default }), targets: Some([0]), position: 0, col_type: Unsigned }]
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
        offset: 61,
        arena_type: ArenaType::Default,
    };

    let actual_arena_subtree = plan
        .formatted_arena_subtree(inner_join_inner_child_id)
        .unwrap();

    let mut expected_arena_subtree = String::new();
    expected_arena_subtree.push_str(
    	r#"---------------------------------------------
[id: 31] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 30
		[id: 30] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 21] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 0, col_type: Integer }]
				[id: 23] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 1, col_type: String }]
				[id: 25] expression: Alias [name = product_units, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 2, col_type: Boolean }]
				[id: 27] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 3, col_type: Unsigned }]
				[id: 29] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 31, arena_type: Default }), targets: None, position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 35] relation: Projection
	Children:
		Child_id = 31
	Output_id: 34
		[id: 34] expression: Row [distribution = Some(Any)]
			List:
				[id: 33] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 35, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 39] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 35
	Output_id: 38
		[id: 38] expression: Row [distribution = Some(Any)]
			List:
				[id: 37] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 39, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
---------------------------------------------
---------------------------------------------
[id: 61] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 39
	Output_id: 60
		[id: 60] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 59] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 61, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
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
[id: 11] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 1] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 0, col_type: Integer }]
				[id: 3] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 1, col_type: String }]
				[id: 5] expression: Alias [name = product_units, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 2, col_type: Boolean }]
				[id: 7] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 3, col_type: Unsigned }]
				[id: 9] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 11, arena_type: Default }), targets: None, position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 24] relation: GroupBy [is_final = false]
	Gr_cols:
		Gr_col: Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 1, col_type: String }
	Children:
		Child_id = 11
	Output_id: 23
		[id: 23] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 14] expression: Alias [name = identification_number, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 0, col_type: Integer }]
				[id: 16] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 1, col_type: String }]
				[id: 18] expression: Alias [name = product_units, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 2, col_type: Boolean }]
				[id: 20] expression: Alias [name = sys_op, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 3, col_type: Unsigned }]
				[id: 22] expression: Alias [name = bucket_id, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 4, col_type: Unsigned }]
---------------------------------------------
---------------------------------------------
[id: 31] relation: Projection
	Children:
		Child_id = 24
	Output_id: 30
		[id: 30] expression: Row [distribution = Some(Any)]
			List:
				[id: 29] expression: Alias [name = column_12, child = Reference { parent: Some(NodeId { offset: 24, arena_type: Default }), targets: Some([0]), position: 1, col_type: String }]
---------------------------------------------
---------------------------------------------
[id: 35] relation: ScanSubQuery
	Children:
		Child_id = 31
	Output_id: 34
		[id: 34] expression: Row [distribution = Some(Any)]
			List:
				[id: 33] expression: Alias [name = column_12, child = Reference { parent: Some(NodeId { offset: 35, arena_type: Default }), targets: Some([0]), position: 0, col_type: String }]
---------------------------------------------
---------------------------------------------
[id: 45] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 35
	Output_id: 44
		[id: 44] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 43] expression: Alias [name = column_12, child = Reference { parent: Some(NodeId { offset: 45, arena_type: Default }), targets: Some([0]), position: 0, col_type: String }]
---------------------------------------------
---------------------------------------------
[id: 40] relation: GroupBy [is_final = true]
	Gr_cols:
		Gr_col: Reference { parent: Some(NodeId { offset: 40, arena_type: Default }), targets: Some([0]), position: 0, col_type: String }
	Children:
		Child_id = 45
	Output_id: 39
		[id: 39] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 38] expression: Alias [name = column_12, child = Reference { parent: Some(NodeId { offset: 40, arena_type: Default }), targets: Some([0]), position: 0, col_type: String }]
---------------------------------------------
---------------------------------------------
[id: 28] relation: Projection
	Children:
		Child_id = 40
	Output_id: 27
		[id: 27] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 26] expression: Alias [name = product_code, child = Reference { parent: Some(NodeId { offset: 28, arena_type: Default }), targets: Some([0]), position: 0, col_type: String }]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}
