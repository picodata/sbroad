use super::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn suggested_distribution() {
    // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
    // with a sec scan and three additional alias-reference pairs
    // for "a", "b" and "c". We want to see, what suggestions would
    // sec scan output row make for a new parent row constructed
    // from different combinations of new "a", "b" and "c".
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("expression")
        .join("suggested_distribution.yaml");
    let s = fs::read_to_string(path).unwrap();
    let plan = Plan::from_yaml(&s).unwrap();

    let a = 11;
    let b = 13;
    let c = 15;
    let scan_output = 8;

    if let Node::Expression(output) = plan.get_node(scan_output).unwrap() {
        // Same order in distribution key
        assert_eq!(
            Distribution::Segment { key: vec![1, 0] },
            output
                .suggested_distribution(&Branch::Left, &[a, b, c], &plan)
                .unwrap()
        );

        // Shuffle distribution key
        assert_eq!(
            Distribution::Segment { key: vec![0, 1] },
            output
                .suggested_distribution(&Branch::Left, &[b, a], &plan)
                .unwrap()
        );

        // Shrink distribution key #1
        assert_eq!(
            Distribution::Random,
            output
                .suggested_distribution(&Branch::Left, &[c, a], &plan)
                .unwrap()
        );

        // Shrink distribution key #2
        assert_eq!(
            Distribution::Random,
            output
                .suggested_distribution(&Branch::Left, &[a], &plan)
                .unwrap()
        );

        // Check both branch mode
        assert_eq!(
            Distribution::Segment { key: vec![1, 0] },
            output
                .suggested_distribution(&Branch::Both, &[a, b, c], &plan)
                .unwrap()
        );

        // Check absent branch int the output
        assert_eq!(
            Distribution::Random,
            output
                .suggested_distribution(&Branch::Right, &[a, b, c], &plan)
                .unwrap()
        );

        //TODO: implement checks for Replicated and Single
    } else {
        panic!("Wrong output node type!");
    }
}
