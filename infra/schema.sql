create extension if not exists "uuid-ossp";


create table if not exists nodes (
    node_id uuid primary key,
    addr text not null,
    last_heartbeat timestamptz not null default now(),
    is_alive boolean not null default true
);


create table if not exists files (
    file_id uuid primary key default uuid_generate_v4(),
    filename text not null,
    size_bytes bigint not null,
    chunk_size_bytes int not null,
    created_at timestamptz not null default now()
);


create table if not exists chunks (
    chunk_id uuid primary key default uuid_generate_v4(),
    file_id uuid not null references files(file_id) on delete cascade,
    chunk_index int not null,
    size_bytes int not null,
    checksum_sha256 char(64) not null
);


create table if not exists replicas (
    chunk_id uuid not null references chunks(chunk_id) on delete cascade,
    node_id uuid not null references nodes(node_id) on delete cascade,
    primary key (chunk_id, node_id)
);


create index if not exists idx_chunks_file_index on chunks(file_id, chunk_index); 