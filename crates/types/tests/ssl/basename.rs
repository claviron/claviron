use super::*;

#[test]
fn only_one_to_eight_letters_a_to_z_form_a_basename() {
    for name in ["a", "default", "claviron"] {
        let basename = Basename::new(name).unwrap();
        assert_eq!(basename.as_str(), name);
        assert_eq!(name.parse::<Basename>().unwrap(), basename);
        assert_eq!(
            Path::new("store").join(basename),
            Path::new("store").join(name)
        );
    }
    for name in [
        "",
        "clavirons",
        "Default",
        "ca1",
        "my-ca",
        "a b",
        "a/b",
        "..",
        ".a",
        "example.test",
        "çay",
    ] {
        assert!(Basename::new(name).is_none(), "{name}");
        assert!(name.parse::<Basename>().is_err(), "{name}");
    }
}
