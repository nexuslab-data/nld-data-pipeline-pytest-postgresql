

CREATE TABLE IF NOT EXISTS test_schema.table_2 (
    id TEXT,
	description TEXT,
	updated_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP at time zone 'utc'),
	CONSTRAINT pk_table_2 PRIMARY KEY (id)
)
;