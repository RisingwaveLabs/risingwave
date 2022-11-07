use smallvec::SmallVec;

pub trait PlanTreeNodeV2 {
    type PlanRef;

    fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]>;
    fn clone_with_inputs(&self, inputs: impl Iterator<Item = Self::PlanRef>) -> Self;
}

macro_rules! impl_plan_tree_node_v2_for_generic_unary_node {
    ($node_type:ident, $input_feild:ident) => {
        impl<P: Clone> crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2
            for $node_type<P>
        {
            type PlanRef = P;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![self.$input_feild.clone()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$input_feild = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                new.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_generic_binary_node {
    ($node_type:ident, $first_input_feild:ident, $second_input_feild:ident) => {
        impl<P: Clone> crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2
            for $node_type<P>
        {
            type PlanRef = P;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![
                    self.$first_input_feild.clone(),
                    self.$second_input_feild.clone()
                ]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$first_input_feild = inputs.next().expect("expect exactly 2 input");
                new.$second_input_feild = inputs.next().expect("expect exactly 2 input");
                assert!(inputs.next().is_none(), "expect exactly 2 input");
                new.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_stream_node_with_core_delegating {
    ($node_type:ident, $core_field:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                self.$core_field.inputs()
            }

            fn clone_with_inputs(&self, inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$core_field = self.$core_field.clone_with_inputs(inputs);
                new
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_stream_leaf_node {
    ($node_type:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                assert!(inputs.next().is_none(), "expect exactly no input");
                self.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_stream_unary_node {
    ($node_type:ident, $input_feild:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![self.$input_feild.clone()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$input_feild = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                new.clone()
            }
        }
    };
}
