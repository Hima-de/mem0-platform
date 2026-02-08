-- PostgreSQL Schema for 10 Million Users
-- Optimized for high-throughput, low-latency operations
-- Uses partitioning, indexing, and connection pooling

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Create custom types
DO $$ BEGIN
    CREATE TYPE memory_category AS ENUM (
        'fact',
        'preference',
        'procedure',
        'insight',
        'context'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE entity_status AS ENUM (
        'active',
        'archived',
        'deleted'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Users table (sharded by user_id hash)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id VARCHAR(255) UNIQUE,
    email VARCHAR(255) UNIQUE,
    plan_tier VARCHAR(50) DEFAULT 'free',
    max_memories INTEGER DEFAULT 1000,
    max_sessions INTEGER DEFAULT 10,
    org_id UUID,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_active_at TIMESTAMPTZ,
    status entity_status DEFAULT 'active'
) PARTITION BY HASH (id);

-- Create partitions for users (64 shards)
DO $$
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS users_%s PARTITION OF users FOR VALUES WITH (modulus 64, remainder %s)',
            i, i
        );
    END LOOP;
END $$;

-- Sessions table (partitioned by created_at month)
CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    session_type VARCHAR(50) DEFAULT 'default',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    status entity_status DEFAULT 'active',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Create monthly partitions for sessions (24 months rolling)
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'sessions_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF sessions
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for sessions
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at DESC);

-- Conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL,
    title VARCHAR(500),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    status entity_status DEFAULT 'active',
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Monthly partitions for conversations
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'conversations_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF conversations
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for conversations
CREATE INDEX IF NOT EXISTS idx_conversations_session_id ON conversations(session_id);
CREATE INDEX IF NOT EXISTS idx_conversations_created_at ON conversations(created_at DESC);

-- Messages table
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    token_count INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status entity_status DEFAULT 'active',
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Monthly partitions for messages
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'messages_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF messages
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for messages
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at DESC);

-- Memories table (main table - heavily used)
CREATE TABLE IF NOT EXISTS memories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    session_id UUID,
    conversation_id UUID,
    message_id UUID,
    content TEXT NOT NULL,
    content_hash VARCHAR(64),  -- For deduplication
    category memory_category DEFAULT 'fact',
    importance_score DECIMAL(3,2) DEFAULT 0.50,
    embedding vector(384),  -- For semantic search (optional)
    metadata JSONB DEFAULT '{}',
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_accessed_at TIMESTAMPTZ,
    access_count INTEGER DEFAULT 0,
    status entity_status DEFAULT 'active',
    expires_at TIMESTAMPTZ,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE SET NULL,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE SET NULL,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE SET NULL
) PARTITION BY RANGE (created_at);

-- Monthly partitions for memories (hot/cold separation)
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'memories_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF memories
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Create hot partition (last 30 days) on SSD
CREATE TABLE IF NOT EXISTS memories_hot (
    LIKE memories INCLUDING ALL
) PARTITION OF memories
FOR VALUES FROM (DATE_TRUNC('month', CURRENT_DATE))
TO (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month'));

-- Create cold partition (older data) on HDD
CREATE TABLE IF NOT EXISTS memories_cold (
    LIKE memories INCLUDING ALL
) PARTITION OF memories
FOR VALUES FROM (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months'))
TO (DATE_TRUNC('month', CURRENT_DATE));

-- Indexes for memories (critical for performance)
CREATE INDEX IF NOT EXISTS idx_memories_user_id ON memories(user_id) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_session_id ON memories(session_id) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_category ON memories(category) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_importance ON memories(importance_score DESC) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_created_at ON memories(created_at DESC) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_user_category ON memories(user_id, category) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_user_importance ON memories(user_id, importance_score DESC) LOCAL;
CREATE INDEX IF NOT EXISTS idx_memories_content_hash ON memories(content_hash) WHERE content_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_memories_tags ON memories USING GIN(tags) WHERE cardinality(tags) > 0;

-- Full-text search index
CREATE INDEX IF NOT EXISTS idx_memories_fts ON memories USING GIN (to_tsvector('english', content));

-- Vector similarity index (using pgvector)
CREATE INDEX IF NOT EXISTS idx_memories_embedding ON memories USING IVFFLAT (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;

-- Embeddings table (for storing pre-computed embeddings)
CREATE TABLE IF NOT EXISTS embeddings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    memory_id UUID NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    embedding vector(384) NOT NULL,
    dimensions INTEGER DEFAULT 384,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Partitions for embeddings
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'embeddings_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF embeddings
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Index for embeddings
CREATE INDEX IF NOT EXISTS idx_embeddings_memory_id ON embeddings(memory_id);
CREATE INDEX IF NOT EXISTS idx_embeddings_model ON embeddings(model_name);

-- Snapshots table
CREATE TABLE IF NOT EXISTS snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    sandbox_id VARCHAR(255) NOT NULL,
    parent_id UUID,
    size_bytes BIGINT DEFAULT 0,
    compressed_size BIGINT DEFAULT 0,
    block_count INTEGER DEFAULT 0,
    content_hash VARCHAR(64) NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    status entity_status DEFAULT 'active',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES snapshots(id) ON DELETE SET NULL
);

-- Indexes for snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_user_id ON snapshots(user_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_content_hash ON snapshots(content_hash);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at DESC);

-- Snapshot blocks table (content-addressable storage references)
CREATE TABLE IF NOT EXISTS snapshot_blocks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_id UUID NOT NULL,
    digest VARCHAR(64) NOT NULL,
    path TEXT NOT NULL,
    is_delta BOOLEAN DEFAULT FALSE,
    size_bytes BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
) PARTITION BY RANGE (created_at);

-- Partitions for snapshot blocks
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'snapshot_blocks_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF snapshot_blocks
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for snapshot blocks
CREATE INDEX IF NOT EXISTS idx_snapshot_blocks_snapshot_id ON snapshot_blocks(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_blocks_digest ON snapshot_blocks(digest);

-- Block storage table (content-addressable storage)
CREATE TABLE IF NOT EXISTS blocks (
    digest VARCHAR(64) PRIMARY KEY,
    data BYTEA NOT NULL,
    compressed_data BYTEA,
    compression_algorithm VARCHAR(20) DEFAULT 'lz4',
    size_bytes BIGINT NOT NULL,
    compressed_size BIGINT,
    reference_count INTEGER DEFAULT 1,
    storage_tier VARCHAR(20) DEFAULT 'hot',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_accessed_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

-- Indexes for blocks
CREATE INDEX IF NOT EXISTS idx_blocks_reference_count ON blocks(reference_count) WHERE reference_count < 2;
CREATE INDEX IF NOT EXISTS idx_blocks_storage_tier ON blocks(storage_tier);
CREATE INDEX IF NOT EXISTS idx_blocks_created_at ON blocks(created_at);
CREATE INDEX IF NOT EXISTS idx_blocks_last_accessed ON blocks(last_accessed_at);

-- Runtime packs table
CREATE TABLE IF NOT EXISTS runtime_packs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    version VARCHAR(20) NOT NULL,
    full_id VARCHAR(130) UNIQUE NOT NULL,
    description TEXT,
    base_image VARCHAR(255) NOT NULL,
    total_size BIGINT NOT NULL,
    cdn_url VARCHAR(500),
    layers JSONB NOT NULL DEFAULT '[]',
    dependencies JSONB NOT NULL DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    status entity_status DEFAULT 'active'
);

-- Indexes for runtime packs
CREATE INDEX IF NOT EXISTS idx_runtime_packs_name_version ON runtime_packs(name, version);
CREATE INDEX IF NOT EXISTS idx_runtime_packs_full_id ON runtime_packs(full_id);

-- User runtime cache table
CREATE TABLE IF NOT EXISTS user_runtime_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    pack_full_id VARCHAR(130) NOT NULL,
    cache_key VARCHAR(255) NOT NULL,
    local_path VARCHAR(500),
    size_bytes BIGINT DEFAULT 0,
    last_used_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (pack_full_id) REFERENCES runtime_packs(full_id) ON DELETE CASCADE
);

-- Indexes for user runtime cache
CREATE INDEX IF NOT EXISTS idx_user_runtime_cache_user_id ON user_runtime_cache(user_id);
CREATE INDEX IF NOT EXISTS idx_user_runtime_cache_pack ON user_runtime_cache(pack_full_id);
CREATE INDEX IF NOT EXISTS idx_user_runtime_cache_expires ON user_runtime_cache(expires_at) WHERE expires_at IS NOT NULL;

-- Analytics events table
CREATE TABLE IF NOT EXISTS analytics_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB DEFAULT '{}',
    session_id UUID,
    duration_ms INTEGER,
    status VARCHAR(20) DEFAULT 'success',
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Monthly partitions for analytics
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'analytics_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF analytics_events
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_analytics_user_id ON analytics_events(user_id);
CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics_events(event_type);
CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_events(created_at DESC);

-- Rate limiting table
CREATE TABLE IF NOT EXISTS rate_limits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    request_count INTEGER DEFAULT 0,
    window_start TIMESTAMPTZ DEFAULT NOW(),
    window_size_seconds INTEGER DEFAULT 60,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Indexes for rate limits
CREATE INDEX IF NOT EXISTS idx_rate_limits_user_endpoint ON rate_limits(user_id, endpoint);
CREATE INDEX IF NOT EXISTS idx_rate_limits_window ON rate_limits(window_start DESC);

-- Audit log table
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    action VARCHAR(100) NOT NULL,
    entity_type VARCHAR(100),
    entity_id UUID,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    request_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Monthly partitions for audit logs
DO $$
DECLARE
    start_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..23 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        partition_name := 'audit_logs_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF audit_logs
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            start_date + INTERVAL '1 month'
        );
    END LOOP;
END $$;

-- Indexes for audit logs
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers
DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN
        SELECT table_name FROM information_schema.columns
        WHERE column_name = 'updated_at'
        AND table_schema = 'public'
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS update_%I_updated_at ON %I', table_name, table_name);
        EXECUTE format('
            CREATE TRIGGER update_%I_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column()',
            table_name, table_name
        );
    END LOOP;
END $$;

-- Create optimized functions
CREATE OR REPLACE FUNCTION get_user_memory_count(user_uuid UUID)
RETURNS INTEGER AS $$
BEGIN
    RETURN (
        SELECT COUNT(*) FROM memories
        WHERE user_id = user_uuid
        AND status = 'active'
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION search_memories(
    user_uuid UUID,
    search_query TEXT,
    category_filter memory_category DEFAULT NULL,
    min_importance DECIMAL DEFAULT 0.0,
    limit_count INTEGER DEFAULT 20,
    offset_count INTEGER DEFAULT 0
)
RETURNS TABLE (
    id UUID,
    content TEXT,
    category memory_category,
    importance_score DECIMAL,
    metadata JSONB,
    created_at TIMESTAMPTZ,
    relevance_score REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        m.id,
        m.content,
        m.category,
        m.importance_score,
        m.metadata,
        m.created_at,
        ts_rank(to_tsvector('english', m.content), plainto_tsquery('english', search_query)) AS relevance_score
    FROM memories m
    WHERE m.user_id = user_uuid
    AND m.status = 'active'
    AND (category_filter IS NULL OR m.category = category_filter)
    AND m.importance_score >= min_importance
    AND (
        search_query = '' OR
        m.content ILIKE '%' || search_query || '%' OR
        to_tsvector('english', m.content) @@ plainto_tsquery('english', search_query)
    )
    ORDER BY
        CASE WHEN search_query = '' THEN m.importance_score ELSE relevance_score END DESC,
        m.created_at DESC
    LIMIT limit_count OFFSET offset_count;
END;
$$ LANGUAGE plpgsql STABLE;

-- Create pg_cron job for cleanup (run daily)
SELECT cron.schedule(
    'daily-memory-cleanup',
    '0 2 * * *',
    $$
    UPDATE memories
    SET status = 'archived',
        updated_at = NOW()
    WHERE status = 'active'
    AND expires_at < NOW()
    AND created_at < NOW() - INTERVAL '90 days';
    $$
);

SELECT cron.schedule(
    'daily-stats-aggregation',
    '0 3 * * *',
    $$
    INSERT INTO analytics_events (user_id, event_type, event_data)
    SELECT
        user_id,
        'daily_stats',
        jsonb_build_object(
            'memory_count', (SELECT COUNT(*) FROM memories WHERE user_id = users.id AND status = 'active'),
            'session_count', (SELECT COUNT(*) FROM sessions WHERE user_id = users.id AND status = 'active'),
            'api_calls', (SELECT COUNT(*) FROM analytics_events WHERE user_id = users.id AND created_at > NOW() - INTERVAL '1 day')
        )
    FROM users
    WHERE users.status = 'active';
    $$
);

-- Comment on tables
COMMENT ON TABLE users IS 'User accounts with quota and tier information';
COMMENT ON TABLE memories IS 'Main memory storage with importance scoring';
COMMENT ON TABLE embeddings IS 'Pre-computed vector embeddings for semantic search';
COMMENT ON TABLE snapshots IS 'Content-addressable filesystem snapshots';
COMMENT ON TABLE blocks IS 'Content-addressable storage blocks with deduplication';
COMMENT ON TABLE runtime_packs IS 'Pre-built runtime environments for sandbox execution';
