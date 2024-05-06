import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		// See https://kit.svelte.dev/docs/adapters for more information about adapters.
		adapter: adapter({
			fallback: 'index.html',
			pages: '../pkg/view/dist'
		}),
		paths: {
			// relative: false,
			// base: ''
		}
	}
};

export default config;
