#[derive(Clone)]
pub struct Options {}

#[derive(Clone, PartialEq)]
pub enum IndexType {
    HashMap,
    BTree,
}
