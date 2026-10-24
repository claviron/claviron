CREATE TABLE hosting_certificates (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    issuer_id INTEGER,
    cn TEXT NOT NULL,
    certs_der BLOB NOT NULL,
    expires_at TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    updated_at TEXT,
    enabled INTEGER NOT NULL,
    revoked_at TEXT
);
CREATE TABLE hosting_certificate_issuers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    acme_account_id INTEGER,
    ca_certificate_id INTEGER
);
INSERT INTO hosting_certificate_issuers VALUES (1, 'Fixture CA', 'local_ca', NULL, 1);
