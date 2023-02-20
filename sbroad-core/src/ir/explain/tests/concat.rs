use super::*;

#[test]
fn concat1_test() {
    explain_check(
        r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#,
        &format!(
            "{}\n{}\n",
            r#"projection (('1'::string) || ('hello') -> "COL_1")"#, r#"    scan "t1""#,
        ),
    );
}

#[test]
fn concat2_test() {
    explain_check(
        r#"SELECT "a" FROM "t1" WHERE CAST('1' as string) || BUCKET_ID('hello') || '2' = 42"#,
        &format!(
            "{}\n{}\n{}\n",
            r#"projection ("t1"."a" -> "a")"#,
            r#"    selection ROW(('1'::string) || (("BUCKET_ID"('hello')) || ('2'))) = ROW(42)"#,
            r#"        scan "t1""#,
        ),
    );
}
