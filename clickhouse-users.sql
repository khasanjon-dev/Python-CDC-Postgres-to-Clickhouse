CREATE TABLE postgres.users
(
    id              UInt32,
    username        String,
    email           String,
    created_at      DateTime,
    codename_schema String,
    is_deleted      UInt8
)
    ENGINE = MergeTree
        ORDER BY (id);
