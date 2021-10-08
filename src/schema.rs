extern crate yaml_rust;
use yaml_rust::YamlLoader;
use self::yaml_rust::yaml;

// TODO: add schema validate

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterSchema{
    schema: yaml::Yaml
}

impl From<String> for ClusterSchema {
    fn from(s: String) -> Self {
        let docs = YamlLoader::load_from_str(&s).unwrap();
        ClusterSchema{
            schema: docs[0].to_owned()
        }
    }
}

impl ClusterSchema {
    pub fn get_sharding_key_by_space(self, space: &str) -> Vec<String> {
        let mut result = Vec::new();
            let spaces = self.schema["spaces"].as_hash().unwrap();

            for (space_name, params) in spaces.iter() {
                let current_space_name = space_name.as_str().unwrap();
                if current_space_name == space {
                    for k in params["sharding_key"].as_vec().unwrap() {
                        result.push(k.as_str().unwrap().to_string().to_lowercase());
                    }
                }
            }
        result
    }
}

#[test]
fn test_yaml_schema_parser() {
    let test_schema = "spaces:
  EMPLOYEES:
    engine: \"memtx\"
    is_local: false
    temporary: false
    format:
      - name: \"ID\"
        is_nullable: false
        type: \"number\"
      - name: \"sysFrom\"
        is_nullable: false
        type: \"number\"
      - name: \"FIRST_NAME\"
        is_nullable: false
        type: \"string\"
      - name: \"sysOp\"
        is_nullable: false
        type: \"number\"
      - name: \"bucket_id\"
        is_nullable: true
        type: \"unsigned\"
    indexes:
      - type: \"TREE\"
        name: \"ID\"
        unique: true
        parts:
          - path: \"ID\"
            type: \"number\"
            is_nullable: false
          - path: \"sysFrom\"
            type: \"number\"
            is_nullable: false
      - type: \"TREE\"
        name: \"bucket_id\"
        unique: false
        parts:
          - path: \"bucket_id\"
            type: \"unsigned\"
            is_nullable: true
    sharding_key:
      - ID
  hash_testing:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"integer\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - identification_number
      - product_code";

    let s = ClusterSchema::from(test_schema.to_string());

    let mut expected_keys = Vec::new();
    expected_keys.push("identification_number".to_string());
    expected_keys.push("product_code".to_string());

    let actual_keys = s.get_sharding_key_by_space("hash_testing");
    assert_eq!(actual_keys, expected_keys)
}
