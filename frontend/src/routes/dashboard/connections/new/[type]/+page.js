/** @type {import('./$types').PageLoad} */
export async function load({ params }) {
    const connectionType = params.type;
    const parameters = await getDestinationParameters(connectionType);

    return {
        connectionType,
        parameters,
    };
}

/** @param {string} connectionType */
async function getDestinationParameters(connectionType) {
    const resource = `/api/destinations/params/${connectionType}`;
    const options = {};
    const response = await fetch(resource, options);
    const parameters = await response.json();

    return parameters;
}
