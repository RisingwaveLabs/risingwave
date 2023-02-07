use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalLimit, LogicalScan, LogicalTopN, PlanTreeNodeUnary};
use crate::optimizer::property::Direction::Asc;
use crate::optimizer::property::{FieldOrder, Order};
use crate::optimizer::PlanRef;

pub struct TopNOnIndexRule {}

impl Rule for TopNOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_topn: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_topn.input().as_logical_scan()?.to_owned();
        let order = logical_topn.topn_order();
        if order.field_order.is_empty() {
            return None;
        }
        let _index = logical_scan.indexes();
        let index = logical_scan.indexes().iter().find(|idx| {
            Order {
                field_order: idx
                    .index_table
                    .pk()
                    .iter()
                    .map(|idx_item| FieldOrder {
                        index: idx_item.index,
                        direct: idx_item.direct,
                    })
                    .collect(),
            }
            .satisfies(order)
        })?;

        let p2s_mapping = index.primary_to_secondary_mapping();

        let index_scan = if logical_scan
            .required_col_idx()
            .iter()
            .all(|x| p2s_mapping.contains_key(x))
        {
            Some(logical_scan.to_index_scan(
                &index.name,
                index.index_table.table_desc().into(),
                p2s_mapping,
            ))
        } else {
            None
        }?;

        let logical_limit = LogicalLimit::create(
            index_scan.into(),
            logical_topn.limit(),
            logical_topn.offset(),
        );
        Some(logical_limit)
    }
}

impl TopNOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(TopNOnIndexRule {})
    }
}
