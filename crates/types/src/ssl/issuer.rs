/// Simple Issuer information extracted from certificate subject or issuer.
/// Only carries country, organization and organizational unit as Strings.
#[derive(Clone, Debug, Default)]
pub struct Issuer {
    pub cn: String,
    pub organization: String,
    pub country: String,
    pub unit: String,
}
impl Issuer {
    pub fn to_org_name(&self) -> String {
        self.organization.clone()
    }

    pub fn to_cname(&self) -> String {
        self.cn.clone()
    }
}
