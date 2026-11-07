/** @type {import('./$types').PageLoad} */
export async function load({ fetch, params }) {
    const connectionType = params.type;
    const parameters = await getDestinationParameters(fetch, connectionType);

    return {
        connectionType,
        parameters,
    };
}

/**
 * @param {typeof globalThis.fetch} fetchFn
 * @param {string} connectionType
 */
async function getDestinationParameters(fetchFn, connectionType) {
    const resource = `/api/destinations/params/${connectionType}`;
    const response = await fetchFn(resource);
    const parameters = await response.json();

    return parameters;
}
