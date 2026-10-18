use claviron_orbit::{Fleet, NodeId};

use super::{
    establish_master_identity, last_master_identity, master_identity_is_known,
};

#[test]
fn generation_counts_master_boots_in_the_shared_ring() {
    let fleet =
        Fleet::join_as("master-generation-test", 3, NodeId::ZERO).unwrap();
    assert_eq!(last_master_identity(&fleet), None);

    let first = establish_master_identity(&fleet).unwrap();
    assert_eq!(first.generation(), 1);
    assert_eq!(first.process().pid(), std::process::id());
    assert!(first.process().is_alive());
    assert_eq!(last_master_identity(&fleet), Some(first));

    let second = establish_master_identity(&fleet).unwrap();
    assert_eq!(second.generation(), 2);
    assert_eq!(second.process(), first.process());
    assert_eq!(last_master_identity(&fleet), Some(second));
    assert!(master_identity_is_known(&fleet, first));
    assert!(master_identity_is_known(&fleet, second));
    assert!(!master_identity_is_known(
        &fleet,
        super::MasterIdentity::new(first.process(), 99)
    ));
}
