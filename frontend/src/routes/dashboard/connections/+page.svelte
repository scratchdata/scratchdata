<script>
    /** @type {import('./$types').PageData} */

    import { fade } from "svelte/transition";

    export let data;

    let visible = false;

    async function f() {
        // /api/destinations
        // await new Promise((r) => setTimeout(r, 500));
        // return "hello";

        return await (await fetch('/api/destinations')).json()
    }
</script>

connections

{data.post.title}

{#await f()}
    <p
        transition:fade
        on:introstart={() => (visible = false)}
        on:outroend={() => (visible = true)}
    >
        waiting
    </p>
{:then x}
    {#if visible}
        <!-- <p in:fade={{ delay: 101, duration: 100 }}>done {x}</p> -->
        <p transition:fade>
            {x[0].Name}
            done 
        </p>
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}
