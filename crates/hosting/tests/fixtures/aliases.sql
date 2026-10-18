CREATE TABLE hosting_servers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE hosting_vhosts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE hosting_vhost_aliases (
    id INTEGER PRIMARY KEY,
    vhost_id INTEGER NOT NULL REFERENCES hosting_vhosts(id) ON DELETE CASCADE ON UPDATE CASCADE,
    hostname TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (vhost_id, hostname)
);
CREATE INDEX idx_vhost_aliases_vhost_id ON hosting_vhost_aliases(vhost_id);

CREATE TABLE hosting_vhost_server (
    vhost_id INTEGER NOT NULL REFERENCES hosting_vhosts(id) ON DELETE CASCADE,
    server_id INTEGER NOT NULL REFERENCES hosting_servers(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (vhost_id, server_id)
);
CREATE INDEX idx_vhost_server_server ON hosting_vhost_server(server_id);

