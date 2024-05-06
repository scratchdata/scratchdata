/** @type {import('./$types').PageLoad} */
export async function load({ fetch, params }) {
    await new Promise(r => setTimeout(r, 2000));
	return {
		post: {
			title: `Title for  goes here`,
            // d: fetch('/api/destinations').json()
            // d: (await fetch('/api/destinations')).json()
		}
	};
}