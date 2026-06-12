package pipeline.authz

default allow = false

# GPU embedding access: only pro and enterprise tenants
allow {
    input.action == "use_gpu"
    input.license_type == "pro"
}

allow {
    input.action == "use_gpu"
    input.license_type == "enterprise"
}

# Milvus collection access: collection must be exactly {tenant_id}_docs
allow {
    input.action == "query_collection"
    input.collection_name == concat("_", [input.tenant_id, "docs"])
}

# Connector type allowlist by license tier
allowed_connectors := {"s3", "nfs"} { input.license_type == "free" }
allowed_connectors := {"s3", "nfs", "database", "stream"} { input.license_type != "free" }

allow {
    input.action == "use_connector"
    input.connector_type == allowed_connectors[_]
}
