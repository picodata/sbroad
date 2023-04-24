use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let mut plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 11] relation: ScanRelation
	Relation: "hash_testing"
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 1] expression: Alias [name = "identification_number", child = Reference { parent: Some(11), targets: None, position: 0 }]
				[id: 3] expression: Alias [name = "product_code", child = Reference { parent: Some(11), targets: None, position: 1 }]
				[id: 5] expression: Alias [name = "product_units", child = Reference { parent: Some(11), targets: None, position: 2 }]
				[id: 7] expression: Alias [name = "sys_op", child = Reference { parent: Some(11), targets: None, position: 3 }]
				[id: 9] expression: Alias [name = "bucket_id", child = Reference { parent: Some(11), targets: None, position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 15] relation: Projection
	Children:
		Child_id = 11
	Output_id: 14
		[id: 14] expression: Row [distribution = Some(Any)]
			List:
				[id: 13] expression: Alias [name = "product_code", child = Reference { parent: Some(15), targets: Some([0]), position: 1 }]
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
    let mut plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 11] relation: ScanRelation
	Relation: "test_space"
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1] expression: Alias [name = "id", child = Reference { parent: Some(11), targets: None, position: 0 }]
				[id: 3] expression: Alias [name = "sysFrom", child = Reference { parent: Some(11), targets: None, position: 1 }]
				[id: 5] expression: Alias [name = "FIRST_NAME", child = Reference { parent: Some(11), targets: None, position: 2 }]
				[id: 7] expression: Alias [name = "sys_op", child = Reference { parent: Some(11), targets: None, position: 3 }]
				[id: 9] expression: Alias [name = "bucket_id", child = Reference { parent: Some(11), targets: None, position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 15] relation: Projection
	Children:
		Child_id = 11
	Output_id: 14
		[id: 14] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 13] expression: Alias [name = "id", child = Reference { parent: Some(15), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 19] relation: ScanSubQuery
	Alias: "t1"
	Children:
		Child_id = 15
	Output_id: 18
		[id: 18] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 17] expression: Alias [name = "id", child = Reference { parent: Some(19), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 31] relation: ScanRelation
	Relation: "hash_testing"
	[No children]
	Output_id: 30
		[id: 30] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 21] expression: Alias [name = "identification_number", child = Reference { parent: Some(31), targets: None, position: 0 }]
				[id: 23] expression: Alias [name = "product_code", child = Reference { parent: Some(31), targets: None, position: 1 }]
				[id: 25] expression: Alias [name = "product_units", child = Reference { parent: Some(31), targets: None, position: 2 }]
				[id: 27] expression: Alias [name = "sys_op", child = Reference { parent: Some(31), targets: None, position: 3 }]
				[id: 29] expression: Alias [name = "bucket_id", child = Reference { parent: Some(31), targets: None, position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 35] relation: Projection
	Children:
		Child_id = 31
	Output_id: 34
		[id: 34] expression: Row [distribution = Some(Any)]
			List:
				[id: 33] expression: Alias [name = "identification_number", child = Reference { parent: Some(35), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 39] relation: ScanSubQuery
	Alias: "t2"
	Children:
		Child_id = 35
	Output_id: 38
		[id: 38] expression: Row [distribution = Some(Any)]
			List:
				[id: 37] expression: Alias [name = "identification_number", child = Reference { parent: Some(39), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 61] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 39
	Output_id: 60
		[id: 60] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 59] expression: Alias [name = "identification_number", child = Reference { parent: Some(61), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 50] relation: InnerJoin
	Condition:
		[id: 57] expression: Bool [op: =]
			Left child
			[id: 55] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
				List:
					[id: 40] expression: Reference
						Alias: "id"
						Referenced table name (or alias): "t1"
						Parent: (current relational node)
						target_id: 0
			Right child
			[id: 56] expression: Row [distribution = Some(Any)]
				List:
					[id: 42] expression: Reference
						Alias: "identification_number"
						Referenced table name (or alias): "t2"
						Parent: (current relational node)
						target_id: 1
	Children:
		Child_id = 19
		Child_id = 61
	Output_id: 49
		[id: 49] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }, Key { positions: [1] }}) })]
			List:
				[id: 46] expression: Alias [name = "id", child = Reference { parent: Some(50), targets: Some([0]), position: 0 }]
				[id: 48] expression: Alias [name = "identification_number", child = Reference { parent: Some(50), targets: Some([1]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 54] relation: Projection
	Children:
		Child_id = 50
	Output_id: 53
		[id: 53] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 52] expression: Alias [name = "id", child = Reference { parent: Some(54), targets: Some([0]), position: 0 }]
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
    let mut plan = sql_to_optimized_ir(query, vec![]);

    // Taken from the expected arena output in the `simple_join` test.
    let inner_join_inner_child_id = 61;

    let actual_arena_subtree = plan
        .formatted_arena_subtree(inner_join_inner_child_id)
        .unwrap();

    let mut expected_arena_subtree = String::new();
    expected_arena_subtree.push_str(
        r#"---------------------------------------------
[id: 31] relation: ScanRelation
	Relation: "hash_testing"
	[No children]
	Output_id: 30
		[id: 30] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 21] expression: Alias [name = "identification_number", child = Reference { parent: Some(31), targets: None, position: 0 }]
				[id: 23] expression: Alias [name = "product_code", child = Reference { parent: Some(31), targets: None, position: 1 }]
				[id: 25] expression: Alias [name = "product_units", child = Reference { parent: Some(31), targets: None, position: 2 }]
				[id: 27] expression: Alias [name = "sys_op", child = Reference { parent: Some(31), targets: None, position: 3 }]
				[id: 29] expression: Alias [name = "bucket_id", child = Reference { parent: Some(31), targets: None, position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 35] relation: Projection
	Children:
		Child_id = 31
	Output_id: 34
		[id: 34] expression: Row [distribution = Some(Any)]
			List:
				[id: 33] expression: Alias [name = "identification_number", child = Reference { parent: Some(35), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 39] relation: ScanSubQuery
	Alias: "t2"
	Children:
		Child_id = 35
	Output_id: 38
		[id: 38] expression: Row [distribution = Some(Any)]
			List:
				[id: 37] expression: Alias [name = "identification_number", child = Reference { parent: Some(39), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 61] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 39
	Output_id: 60
		[id: 60] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 59] expression: Alias [name = "identification_number", child = Reference { parent: Some(61), targets: Some([0]), position: 0 }]
---------------------------------------------
"#);

    assert_eq!(expected_arena_subtree, actual_arena_subtree);
}

#[test]
fn simple_aggregation_with_group_by() {
    let query = r#"SELECT "product_code" FROM "hash_testing" GROUP BY "product_code""#;
    let mut plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 11] relation: ScanRelation
	Relation: "hash_testing"
	[No children]
	Output_id: 10
		[id: 10] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 1] expression: Alias [name = "identification_number", child = Reference { parent: Some(11), targets: None, position: 0 }]
				[id: 3] expression: Alias [name = "product_code", child = Reference { parent: Some(11), targets: None, position: 1 }]
				[id: 5] expression: Alias [name = "product_units", child = Reference { parent: Some(11), targets: None, position: 2 }]
				[id: 7] expression: Alias [name = "sys_op", child = Reference { parent: Some(11), targets: None, position: 3 }]
				[id: 9] expression: Alias [name = "bucket_id", child = Reference { parent: Some(11), targets: None, position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 24] relation: GroupBy [is_final = false]
	Gr_cols:
		Gr_col: Reference { parent: Some(24), targets: Some([0]), position: 1 }
	Children:
		Child_id = 11
	Output_id: 23
		[id: 23] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 14] expression: Alias [name = "identification_number", child = Reference { parent: Some(24), targets: Some([0]), position: 0 }]
				[id: 16] expression: Alias [name = "product_code", child = Reference { parent: Some(24), targets: Some([0]), position: 1 }]
				[id: 18] expression: Alias [name = "product_units", child = Reference { parent: Some(24), targets: Some([0]), position: 2 }]
				[id: 20] expression: Alias [name = "sys_op", child = Reference { parent: Some(24), targets: Some([0]), position: 3 }]
				[id: 22] expression: Alias [name = "bucket_id", child = Reference { parent: Some(24), targets: Some([0]), position: 4 }]
---------------------------------------------
---------------------------------------------
[id: 29] relation: Projection
	Children:
		Child_id = 24
	Output_id: 28
		[id: 28] expression: Row [distribution = Some(Any)]
			List:
				[id: 27] expression: Alias [name = "column_12", child = Reference { parent: Some(24), targets: Some([0]), position: 1 }]
---------------------------------------------
---------------------------------------------
[id: 33] relation: ScanSubQuery
	Children:
		Child_id = 29
	Output_id: 32
		[id: 32] expression: Row [distribution = Some(Any)]
			List:
				[id: 31] expression: Alias [name = "column_12", child = Reference { parent: Some(33), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 45] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] })]
	Children:
		Child_id = 33
	Output_id: 44
		[id: 44] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 43] expression: Alias [name = "column_12", child = Reference { parent: Some(45), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 38] relation: GroupBy [is_final = true]
	Gr_cols:
		Gr_col: Reference { parent: Some(38), targets: Some([0]), position: 0 }
	Children:
		Child_id = 45
	Output_id: 37
		[id: 37] expression: Row [distribution = Some(Any)]
			List:
				[id: 36] expression: Alias [name = "column_12", child = Reference { parent: Some(38), targets: Some([0]), position: 0 }]
---------------------------------------------
---------------------------------------------
[id: 41] relation: Projection
	Children:
		Child_id = 38
	Output_id: 40
		[id: 40] expression: Row [distribution = Some(Any)]
			List:
				[id: 26] expression: Alias [name = "product_code", child = Reference { parent: Some(41), targets: Some([0]), position: 0 }]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}
