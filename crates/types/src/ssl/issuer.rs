use x509_parser::certificate::X509Certificate;

use super::Error;

/// Who signed a certificate, read from its issuer name.
///
/// A view: every attribute borrows from the certificate's DER and nothing is
/// copied. An absent attribute is `None`, not an empty string.
#[derive(Clone, Copy, Debug, Default)]
pub struct Issuer<'a> {
    pub cn: Option<&'a str>,
    pub organization: Option<&'a str>,
    pub country: Option<&'a str>,
    pub unit: Option<&'a str>,
}

impl<'a> TryFrom<&'a X509Certificate<'a>> for Issuer<'a> {
    type Error = Error;

    fn try_from(cert: &'a X509Certificate<'a>) -> Result<Self, Self::Error> {
        let name = cert.issuer();
        let text = |attribute: Option<
            &'a x509_parser::x509::AttributeTypeAndValue<'a>,
        >| {
            attribute.map(|value| value.as_str()).transpose()
        };
        Ok(Self {
            cn: text(name.iter_common_name().next())?,
            organization: text(name.iter_organization().next())?,
            country: text(name.iter_country().next())?,
            unit: text(name.iter_organizational_unit().next())?,
        })
    }
}
