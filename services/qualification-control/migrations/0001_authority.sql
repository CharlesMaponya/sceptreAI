-- Apply once to an empty, dedicated qualification database using psql -1 -v ON_ERROR_STOP=1.

CREATE TABLE final_test_allocations (
	split_digest VARCHAR(128) NOT NULL,
	project_reference VARCHAR(255) NOT NULL,
	scope_id UUID NOT NULL,
	canonical_provider VARCHAR(32) NOT NULL,
	provider_manifest_digest VARCHAR(128) NOT NULL,
	status VARCHAR(9) DEFAULT 'allocated' NOT NULL,
	cas_version INTEGER DEFAULT '0' NOT NULL,
	opened_at TIMESTAMP WITH TIME ZONE,
	committed_at TIMESTAMP WITH TIME ZONE,
	result_digest VARCHAR(128),
	terminal_reason TEXT,
	id UUID NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_final_test_allocations PRIMARY KEY (id),
	CONSTRAINT uq_final_test_split_digest UNIQUE (split_digest),
	CONSTRAINT uq_final_test_scope UNIQUE (scope_id)
);

CREATE INDEX ix_final_test_status ON final_test_allocations (status, updated_at);

CREATE TABLE final_test_authority_receipts (
	allocation_id UUID NOT NULL,
	operation VARCHAR(32) NOT NULL,
	provider VARCHAR(32) NOT NULL,
	request_digest VARCHAR(128) NOT NULL,
	receipt_digest VARCHAR(128) NOT NULL,
	signature_algorithm VARCHAR(32) NOT NULL,
	signature TEXT NOT NULL,
	payload JSONB DEFAULT '{}'::jsonb NOT NULL,
	id UUID NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_final_test_authority_receipts PRIMARY KEY (id),
	CONSTRAINT uq_final_test_receipt_operation UNIQUE (allocation_id, operation),
	CONSTRAINT uq_final_test_receipt_digest UNIQUE (receipt_digest),
	CONSTRAINT fk_final_test_authority_receipts_allocation_id_final_te_371d FOREIGN KEY(allocation_id) REFERENCES final_test_allocations (id) ON DELETE CASCADE
);
