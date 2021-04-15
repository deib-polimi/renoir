use crate::block::NextStrategy;
use crate::operator::Data;
use crate::stream::BlockId;

#[derive(Clone, Debug)]
pub struct DataType(String);

#[derive(Clone, Debug)]
pub struct BlockStructure {
    operators: Vec<OperatorStructure>,
}

#[derive(Clone, Debug)]
pub struct OperatorStructure {
    pub title: String,
    pub kind: OperatorKind,
    pub receivers: Vec<OperatorReceiver>,
    pub connections: Vec<Connection>,
    pub out_type: DataType,
}

#[derive(Clone, Debug)]
pub enum OperatorKind {
    Operator,
    Sink,
    Source,
}

#[derive(Clone, Debug)]
pub struct OperatorReceiver {
    pub previous_block_id: BlockId,
    pub data_type: DataType,
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub to_block_id: BlockId,
    pub data_type: DataType,
    pub strategy: ConnectionStrategy,
}

#[derive(Clone, Debug)]
pub enum ConnectionStrategy {
    OnlyOne,
    Random,
    GroupBy,
}

impl DataType {
    pub fn of<T: ?Sized>() -> Self {
        let type_name = std::any::type_name::<T>();
        Self(DataType::clean_str(type_name))
    }

    fn clean_str(s: &str) -> String {
        let mut result = "".to_string();
        let mut current_ident = "".to_string();
        for c in s.chars() {
            // c is part of an identifier
            if c.is_alphanumeric() {
                current_ident.push(c);
                // the current identifier was just a type path
            } else if c == ':' {
                current_ident = "".to_string();
                // other characters like space, <, >
            } else {
                // if there was an identifier, this character marks its end
                if !current_ident.is_empty() {
                    result += &current_ident;
                    current_ident = "".to_string();
                }
                result.push(c);
            }
        }
        if !current_ident.is_empty() {
            result += &current_ident;
        }
        result
    }
}

impl BlockStructure {
    pub fn new() -> Self {
        Self {
            operators: Default::default(),
        }
    }

    pub fn add_operator(mut self, operator: OperatorStructure) -> Self {
        self.operators.push(operator);
        self
    }
}

impl OperatorStructure {
    pub fn new<Out: ?Sized, S: Into<String>>(title: S) -> Self {
        Self {
            title: title.into(),
            kind: OperatorKind::Operator,
            receivers: Default::default(),
            connections: Default::default(),
            out_type: DataType::of::<Out>(),
        }
    }
}

impl OperatorReceiver {
    pub fn new<T: ?Sized>(previous_block_id: BlockId) -> Self {
        Self {
            previous_block_id,
            data_type: DataType::of::<T>(),
        }
    }
}

impl Connection {
    pub(crate) fn new<T: Data>(to_block_id: BlockId, strategy: &NextStrategy<T>) -> Self {
        Self {
            to_block_id,
            data_type: DataType::of::<T>(),
            strategy: strategy.into(),
        }
    }
}

impl<Out: Data> From<&NextStrategy<Out>> for ConnectionStrategy {
    fn from(strategy: &NextStrategy<Out>) -> Self {
        match strategy {
            NextStrategy::OnlyOne => ConnectionStrategy::OnlyOne,
            NextStrategy::Random => ConnectionStrategy::Random,
            NextStrategy::GroupBy(_) => ConnectionStrategy::GroupBy,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::DataType;

    #[test]
    fn test_data_type_clean() {
        let dataset = [
            ("aaa", "aaa"),
            ("aaa::bbb::ccc", "ccc"),
            ("(aaa, bbb::ccc::ddd)", "(aaa, ddd)"),
            ("aaa<bbb>", "aaa<bbb>"),
            ("aaa::bbb::ccc<ddd::eee>", "ccc<eee>"),
            ("aaa::bbb<ccc::ddd<eee>>", "bbb<ddd<eee>>"),
        ];
        for (input, expected) in &dataset {
            assert_eq!(&DataType::clean_str(input), expected);
        }
    }
}
