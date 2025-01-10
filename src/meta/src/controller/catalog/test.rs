// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod tests {
    use risingwave_pb::catalog::StreamSourceInfo;

    use crate::controller::catalog::*;

    const TEST_DATABASE_ID: DatabaseId = 1;
    const TEST_SCHEMA_ID: SchemaId = 2;
    const TEST_OWNER_ID: UserId = 1;

    #[tokio::test]
    async fn test_database_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_database = PbDatabase {
            name: "db1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_database(pb_database).await?;

        let database_id: DatabaseId = Database::find()
            .select_only()
            .column(database::Column::DatabaseId)
            .filter(database::Column::Name.eq("db1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Database, database_id, "db2")
            .await?;
        let database = Database::find_by_id(database_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(database.name, "db2");

        mgr.drop_object(ObjectType::Database, database_id, DropMode::Cascade)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_schema = PbSchema {
            database_id: TEST_DATABASE_ID as _,
            name: "schema1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_schema(pb_schema.clone()).await?;
        assert!(mgr.create_schema(pb_schema).await.is_err());

        let schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("schema1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Schema, schema_id, "schema2")
            .await?;
        let schema = Schema::find_by_id(schema_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(schema.name, "schema2");
        mgr.drop_object(ObjectType::Schema, schema_id, DropMode::Restrict)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_view() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view AS SELECT 1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view.clone()).await?;
        assert!(mgr.create_view(pb_view).await.is_err());

        let view = View::find().one(&mgr.inner.read().await.db).await?.unwrap();
        mgr.drop_object(ObjectType::View, view.view_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view.view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_function() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let test_data_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Int32 as _,
            ..Default::default()
        };
        let arg_types = vec![test_data_type.clone()];
        let pb_function = PbFunction {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "test_function".to_owned(),
            owner: TEST_OWNER_ID as _,
            arg_types,
            return_type: Some(test_data_type.clone()),
            language: "python".to_owned(),
            kind: Some(risingwave_pb::catalog::function::Kind::Scalar(
                Default::default(),
            )),
            ..Default::default()
        };
        mgr.create_function(pb_function.clone()).await?;
        assert!(mgr.create_function(pb_function).await.is_err());

        let function = Function::find()
            .inner_join(Object)
            .filter(
                object::Column::DatabaseId
                    .eq(TEST_DATABASE_ID)
                    .and(object::Column::SchemaId.eq(TEST_SCHEMA_ID))
                    .add(function::Column::Name.eq("test_function")),
            )
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(function.return_type.to_protobuf(), test_data_type);
        assert_eq!(function.arg_types.to_protobuf().len(), 1);
        assert_eq!(function.language, "python");

        mgr.drop_object(
            ObjectType::Function,
            function.function_id,
            DropMode::Restrict,
        )
        .await?;
        assert!(Object::find_by_id(function.function_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_relation_rename() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_source = PbSource {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "s1".to_owned(),
            owner: TEST_OWNER_ID as _,
            definition: r#"CREATE SOURCE s1 (v1 int) with (
  connector = 'kafka',
  topic = 'kafka_alter',
  properties.bootstrap.server = 'message_queue:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON"#
                .to_owned(),
            info: Some(StreamSourceInfo {
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.create_source(pb_source).await?;
        let source_id: SourceId = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .filter(source::Column::Name.eq("s1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view_1".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view_1 AS SELECT v1 FROM s1".to_owned(),
            dependent_relations: vec![source_id as _],
            ..Default::default()
        };
        mgr.create_view(pb_view).await?;
        let view_id: ViewId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("view_1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Source, source_id, "s2").await?;
        let source = Source::find_by_id(source_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(source.name, "s2");
        assert_eq!(
            source.definition,
            "CREATE SOURCE s2 (v1 INT) WITH (\
  connector = 'kafka', \
  topic = 'kafka_alter', \
  properties.bootstrap.server = 'message_queue:29092', \
  scan.startup.mode = 'earliest'\
) FORMAT PLAIN ENCODE JSON"
        );

        let view = View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(
            view.definition,
            "CREATE VIEW view_1 AS SELECT v1 FROM s2 AS s1"
        );

        mgr.drop_object(ObjectType::Source, source_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }
}
