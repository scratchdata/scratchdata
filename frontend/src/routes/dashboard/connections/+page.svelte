<script>
    /** @type {import('./$types').PageData} */

    import { fade } from "svelte/transition";

    export let data;

    let visible = true;

    async function getDestinations() {
        // /api/destinations
        // await new Promise((r) => setTimeout(r, 500));
        // return "hello";

        return await (await fetch('/api/destinations')).json()
    }

    let destinations = getDestinations();
</script>

connections

{#await destinations}
    <!-- <p
        transition:fade
        on:introstart={() => (visible = false)}
        on:outroend={() => (visible = true)}
    >
        waiting
    </p> -->
{:then destList}
    {#if visible}
        <!-- <p in:fade={{ delay: 101, duration: 100 }}>done {x}</p> -->
        <!-- <p transition:fade> -->
            {#each destList as dest}
            <a href="/dashboard/connections/edit/{dest.ID}">{dest.Name}</a>
            {/each}
            <!-- done  -->
        <!-- </p> -->
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}
