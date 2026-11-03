use super::*;

struct Reader {
    directory: Arc<Http01ChallengeDirectory>,
    seen: Mutex<Vec<Option<Arc<PathBuf>>>>,
}

impl Http01DirectoryObserver for Reader {
    fn directory_changed(&self) {
        // Publication releases the subscription lock before invoking readers.
        let _subscriptions = self.directory.observers.lock().unwrap();
        self.seen.lock().unwrap().push(self.directory.snapshot());
    }
}

#[test]
fn http01_directory_notifies_changes_once_and_keeps_pinned_generations() {
    let directory = Arc::new(Http01ChallengeDirectory::default());
    let reader = Arc::new(Reader {
        directory: directory.clone(),
        seen: Mutex::default(),
    });
    let observer: Arc<dyn Http01DirectoryObserver> = reader.clone();
    let weak = Arc::downgrade(&observer);
    directory.observe(weak.clone());
    directory.observe(weak.clone());
    directory.publish(None);
    assert!(reader.seen.lock().unwrap().is_empty());

    directory.publish(Some(PathBuf::from("first")));
    let first = directory.snapshot().unwrap();
    directory.publish(Some(PathBuf::from("first")));
    assert_eq!(reader.seen.lock().unwrap().len(), 1);
    assert!(Arc::ptr_eq(&first, &directory.snapshot().unwrap()));

    directory.publish(Some(PathBuf::from("second")));
    directory.publish(None);
    let seen = reader.seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert!(Arc::ptr_eq(seen[0].as_ref().unwrap(), &first));
    assert_eq!(seen[1].as_deref(), Some(&PathBuf::from("second")));
    assert!(seen[2].is_none());
    assert_eq!(first.as_path(), std::path::Path::new("first"));
    drop(seen);

    directory.unobserve(&weak);
    directory.publish(Some(PathBuf::from("third")));
    assert_eq!(reader.seen.lock().unwrap().len(), 3);
}

#[test]
fn http01_directory_subscriptions_do_not_retain_readers() {
    let directory = Arc::new(Http01ChallengeDirectory::default());
    let reader = Arc::new(Reader {
        directory: directory.clone(),
        seen: Mutex::default(),
    });
    let observer: Arc<dyn Http01DirectoryObserver> = reader;
    let weak = Arc::downgrade(&observer);
    directory.observe(weak.clone());
    drop(observer);
    assert!(weak.upgrade().is_none());
    directory.publish(Some(PathBuf::from("tokens")));
    assert!(directory.observers.lock().unwrap().is_empty());
}
