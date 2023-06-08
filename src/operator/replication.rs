#[cfg(test)]
mod tests {
    use crate::block::Replication;
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::test::FakeOperator;

    #[test]
    fn test_replication() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let operator = FakeOperator::<u8>::empty();
        let stream = env.stream(operator);
        let old_block_id = stream.block.id;
        let new_stream = stream.replication(Replication::new_limited(42));
        let new_block_id = new_stream.block.id;
        assert_eq!(
            new_stream.block.scheduler_requirements.replication,
            Replication::new_limited(42)
        );
        assert_ne!(old_block_id, new_block_id);
    }
}
