use decimal::d128;

use crate::{
    executor::engine::{mock::EngineMock, Engine},
    ir::value::Value as IrValue,
    parser::extargs::BucketCalcArgsDict,
};

#[test]
fn bucket_calc_args() {
    let engine = EngineMock::new();
    let args = BucketCalcArgsDict {
        space: "EMPLOYEES".into(),
        rec: [(String::from("ID"), IrValue::Number(d128!(100.0)))]
            .iter()
            .cloned()
            .collect(),
    };
    let filtered_args = engine
        .extract_sharding_keys(args.space, args.rec)
        .unwrap()
        .into_iter()
        .fold(String::new(), |mut acc, v| {
            let s: String = v.into();
            acc.push_str(s.as_str());
            acc
        });

    assert_eq!(engine.determine_bucket_id(&filtered_args), 2377);

    let args = BucketCalcArgsDict {
        space: "hash_testing".into(),
        rec: [
            (
                String::from("identification_number"),
                IrValue::Number(d128!(93312)),
            ),
            (
                String::from("product_code"),
                IrValue::String("fff100af".into()),
            ),
            (String::from("product_units"), IrValue::Number(d128!(10))),
            (String::from("sys_op"), IrValue::Number(d128!(981.945))),
        ]
        .iter()
        .cloned()
        .collect(),
    };
    let filtered_args = engine
        .extract_sharding_keys(args.space, args.rec)
        .unwrap()
        .into_iter()
        .fold(String::new(), |mut acc, v| {
            let s: String = v.into();
            acc.push_str(s.as_str());
            acc
        });
    assert_eq!(engine.determine_bucket_id(&filtered_args), 7704);
}
