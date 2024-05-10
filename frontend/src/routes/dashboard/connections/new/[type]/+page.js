const connectionTypeToTitle = {
    bigquery: "BigQuery",
    clickhouse: "ClickHouse",
    duckdb: "DuckDB",
    postgres: "Postgres",
    redshift: "RedShift",
};

/** @type {import('./$types').PageLoad} */
export async function load({ params }) {
    const connectionType = params.type;
    const connectionTitle = connectionTypeToTitle[connectionType];
    const parameters = await getDestinationParameters(connectionType);

    return {
        connectionType,
        connectionTitle,
        parameters,
    };
}

async function getDestinationParameters(connectionType) {
    const resource = `/api/destinations/params/${connectionType}`;
    const options = {};
    const response = await fetch(resource, options);
    const parameters = await response.json();

    return parameters;
}
