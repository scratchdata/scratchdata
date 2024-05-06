<script>
    /** @type {import('./$types').PageData} */

    import { fade } from "svelte/transition";

    export let data;

    let visible = false;

    async function f() {
        await new Promise((r) => setTimeout(r, 2000));
        return "hello";
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
        <p in:fade={{ delay: 101, duration: 100 }}>done {x}</p>
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}
