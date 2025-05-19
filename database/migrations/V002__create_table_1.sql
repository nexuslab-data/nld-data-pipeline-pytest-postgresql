

CREATE TABLE IF NOT EXISTS test_schema.table_1 (
    id TEXT,
	description TEXT,
	updated_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP at time zone 'utc'),
	CONSTRAINT pk_table_1 PRIMARY KEY (id)
)
;