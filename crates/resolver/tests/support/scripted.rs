use std::collections::VecDeque;
use std::sync::Mutex;

use claviron_resolver::{
    Answer, Query, RecordType, ResolveError, ResolveFuture, Resolver,
};

type Step = (&'static str, RecordType, Result<Answer, ResolveError>);

pub struct ScriptedResolver {
    pub steps: Mutex<VecDeque<Step>>,
}
impl Resolver for ScriptedResolver {
    fn lookup<'a>(
        &'a self,
        query: Query<'a>,
    ) -> ResolveFuture<'a> {
        let (name, record_type, result) =
            self.steps.lock().unwrap().pop_front().expect("unexpected query");
        assert_eq!((query.name, query.record_type), (name, record_type));
        Box::pin(std::future::ready(result))
    }
}
