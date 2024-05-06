/** @type {import('./$types').PageLoad} */
export async function load({ params }) {
    await new Promise(r => setTimeout(r, 2000));
	return {
		post: {
			title: `Title for  goes here`,
		}
	};
}