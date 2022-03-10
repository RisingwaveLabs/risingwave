use paste::paste;

use super::super::plan_node::*;
use super::super::property::Convention;
use crate::for_all_plan_nodes;

/// Define `PlanVisitor` trait.
macro_rules! def_visitor {
    ([], $({ $convention:ident, $name:ident }),*) => {
        /// The visitor for plan nodes. visit all inputs and return the ret value of the left most input,
        /// and leaf node returns R::default()
        pub trait PlanVisitor<R:Default> {
            fn check_convention(&self, _convention: Convention) -> bool {
                return true;
            }
            paste! {
                fn visit(&mut self, plan: PlanRef) -> R{
                    match plan.node_type() {
                        $(
                            PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
                        )*
                    }
                }

                $(
                    #[doc = "Visit [`" [<$convention $name>] "`] , the function should visit the inputs."]
                    fn [<visit_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> R {
                        let inputs = plan.inputs();
                        if inputs.is_empty() {
                            return R::default();
                        }
                        let mut iter = plan.inputs().into_iter();
                        let ret = self.visit(iter.next().unwrap());
                        iter.for_each(|input| {self.visit(input);});
                        ret
                    }
                )*
            }
        }
    }
}

for_all_plan_nodes! { def_visitor }
