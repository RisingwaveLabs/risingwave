// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::{anyhow, Result};
use itertools::Itertools;
use yaml_rust::{Yaml, YamlEmitter, YamlLoader};

use crate::ServiceConfig;

mod dollar_expander;
mod id_expander;
mod provide_expander;
mod use_expander;
use dollar_expander::DollarExpander;
use id_expander::IdExpander;
use provide_expander::ProvideExpander;
use use_expander::UseExpander;

pub struct ConfigExpander;

impl ConfigExpander {
    /// Transforms `risedev.yml` to a fully expanded yaml file.
    ///
    /// * `config` is the full content of `risedev.yml`.
    /// * `profile` is the selected config profile called by `risedev dev <profile>`. It is one of
    ///   the keys in the `risedev` section.
    pub fn expand(config: &str, profile: &str) -> Result<(Option<String>, Yaml)> {
        Self::expand_with_extra_info(config, profile, HashMap::new())
    }

    /// * `extra_info` is additional variables for variable expansion by [`DollarExpander`].
    ///
    /// See [`ConfigExpander::expand`] for other information.
    pub fn expand_with_extra_info(
        config: &str,
        profile: &str,
        extra_info: HashMap<String, String>,
    ) -> Result<(Option<String>, Yaml)> {
        let [config]: [_; 1] = YamlLoader::load_from_str(config)?
            .try_into()
            .map_err(|_| anyhow!("expect yaml config to have only one section"))?;

        let mut risingwave_config_path = None;

        let global_config = config
            .as_hash()
            .ok_or_else(|| anyhow!("expect config to be a hashmap"))?;
        let risedev_section = global_config
            .get(&Yaml::String("risedev".to_string()))
            .ok_or_else(|| anyhow!("expect `risedev` section"))?;
        let risedev_section = risedev_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect `risedev` section to be a hashmap"))?;
        let template_section = global_config
            .get(&Yaml::String("template".to_string()))
            .ok_or_else(|| anyhow!("expect `risedev` section"))?;
        // selected and expanded profile config.
        let expanded_config: Vec<(Yaml, Yaml)> = risedev_section
            .iter()
            .filter(|(k, _)| k == &&Yaml::String(profile.to_string()))
            .map(|(profile, v)| {
                profile
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `risedev` section to use string key"))?;
                let map = v
                    .as_hash()
                    .ok_or_else(|| anyhow!("expect `risedev` section to be a hashmap"))?;

                if let Some(config_path) = map.get(&Yaml::String("config-path".to_string())) {
                    let config_path = config_path
                        .as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| anyhow!("expect `config-path` to be a string"))?;
                    risingwave_config_path = Some(config_path.clone());
                    update_config(config_path)?;
                }

                let v = map
                    .get(&Yaml::String("steps".to_string()))
                    .ok_or_else(|| anyhow!("expect `steps` section"))?;
                let mut use_expander = UseExpander::new(template_section)?;
                let v = use_expander.visit(v.clone())?;
                let mut dollar_expander = DollarExpander::new(extra_info.clone());
                let v = dollar_expander.visit(v)?;
                let mut id_expander = IdExpander::new(&v)?;
                let v = id_expander.visit(v)?;
                let mut provide_expander = ProvideExpander::new(&v)?;
                let v = provide_expander.visit(v)?;
                Ok::<_, anyhow::Error>((profile.clone(), v))
            })
            .try_collect()?;

        assert!(
            expanded_config.len() == 1,
            "`risedev` section key should be unique"
        );
        Ok((
            risingwave_config_path,
            Yaml::Hash(expanded_config.into_iter().collect()),
        ))
    }

    /// Parses the expanded yaml into [`ServiceConfig`]s.
    /// The order is the same as the original array's order.
    pub fn deserialize(expanded_config: &Yaml, profile: &str) -> Result<Vec<ServiceConfig>> {
        let risedev_section = expanded_config
            .as_hash()
            .ok_or_else(|| anyhow!("expect risedev section to be a hashmap"))?;
        let scene = risedev_section
            .get(&Yaml::String(profile.to_string()))
            .ok_or_else(|| anyhow!("{} not found", profile))?;
        let steps = scene
            .as_vec()
            .ok_or_else(|| anyhow!("expect steps to be an array"))?;
        let config: Vec<ServiceConfig> = steps
            .iter()
            .map(|step| {
                let use_type = step
                    .as_hash()
                    .ok_or_else(|| anyhow!("expect step to be a hashmap"))?;
                let use_type = use_type
                    .get(&Yaml::String("use".to_string()))
                    .ok_or_else(|| anyhow!("expect `use` in step"))?;
                let use_type = use_type
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `use` to be a string"))?
                    .to_string();
                let mut out_str = String::new();
                let mut emitter = YamlEmitter::new(&mut out_str);
                emitter.dump(step)?;
                let result = match use_type.as_str() {
                    "minio" => ServiceConfig::Minio(serde_yaml::from_str(&out_str)?),
                    "etcd" => ServiceConfig::Etcd(serde_yaml::from_str(&out_str)?),
                    "frontend" => ServiceConfig::Frontend(serde_yaml::from_str(&out_str)?),
                    "compactor" => ServiceConfig::Compactor(serde_yaml::from_str(&out_str)?),
                    "compute-node" => ServiceConfig::ComputeNode(serde_yaml::from_str(&out_str)?),
                    "meta-node" => ServiceConfig::MetaNode(serde_yaml::from_str(&out_str)?),
                    "prometheus" => ServiceConfig::Prometheus(serde_yaml::from_str(&out_str)?),
                    "grafana" => ServiceConfig::Grafana(serde_yaml::from_str(&out_str)?),
                    "jaeger" => ServiceConfig::Jaeger(serde_yaml::from_str(&out_str)?),
                    "aws-s3" => ServiceConfig::AwsS3(serde_yaml::from_str(&out_str)?),
                    "kafka" => ServiceConfig::Kafka(serde_yaml::from_str(&out_str)?),
                    "pubsub" => ServiceConfig::Pubsub(serde_yaml::from_str(&out_str)?),
                    "redis" => ServiceConfig::Redis(serde_yaml::from_str(&out_str)?),
                    "connector-node" => {
                        ServiceConfig::ConnectorNode(serde_yaml::from_str(&out_str)?)
                    }
                    "zookeeper" => ServiceConfig::ZooKeeper(serde_yaml::from_str(&out_str)?),
                    "redpanda" => ServiceConfig::RedPanda(serde_yaml::from_str(&out_str)?),
                    other => return Err(anyhow!("unsupported use type: {}", other)),
                };
                Ok(result)
            })
            .try_collect()?;

        let mut services = HashMap::new();
        for x in &config {
            let id = x.id().to_string();
            if services.insert(id.clone(), x).is_some() {
                return Err(anyhow!("duplicate id: {}", id));
            }
        }
        Ok(config)
    }
}

fn update_config(provided_path: impl AsRef<Path>) -> Result<()> {
    let base_path = Path::new(&env::var("PREFIX_CONFIG")?).join("risingwave.toml");

    // Update the content in `base_path` with *additional* content in `provided_path`.
    let mut config: toml::Value = toml::from_str(&std::fs::read_to_string(&base_path)?)?;
    let provided_config: toml::Value = toml::from_str(&std::fs::read_to_string(&provided_path)?)?;
    merge_toml(&mut config, &provided_config);

    std::fs::write(base_path, config.to_string())?;

    Ok(())
}

/// * For tables, we recursively update or insert new values for each key.
/// * For other types, (including array/array of tables), we simply replace the old with the new
///   one.
fn merge_toml(base: &mut toml::Value, provided: &toml::Value) {
    match (base, provided) {
        (toml::Value::Table(base_table), toml::Value::Table(provided_table)) => {
            for (k, v) in provided_table.iter() {
                match base_table.get_mut(k) {
                    Some(x) => merge_toml(x, v),
                    None => {
                        let _ = base_table.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (base, provided) => *base = provided.clone(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_merge_toml() {
        let mut base: toml::Value = toml::from_str(
            r#"
[foo]
foo = 0
a = 1
b = [1, 2, 3]

[bar]
c = { c1 = "a", c2 = "b" }

[foo.bar]
d = 114514

[[foobar]]
e = 1919810

[[foobar]]
f = 810
"#,
        )
        .unwrap();

        let provided: toml::Value = toml::from_str(
            r#"
[foo]
a = 2
b = "3"

[bar]
c = { c1 = 0, c3 = 'd' }

[foo.bar]
e = "abc"

[[foobar]]
boom = 0
"#,
        )
        .unwrap();

        let expected: toml::Value = toml::from_str(
            r#"
[foo]
foo = 0
a = 2
b = "3"

[bar]
c = { c1 = 0, c2 = "b", c3 = 'd' }

[foo.bar]
d = 114514
e = "abc"

[[foobar]]
boom = 0
"#,
        )
        .unwrap();

        super::merge_toml(&mut base, &provided);
        assert_eq!(base, expected);
    }
}
