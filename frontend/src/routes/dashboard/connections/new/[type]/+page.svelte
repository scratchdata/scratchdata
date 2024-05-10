<script lang="ts">
    import DestinationForm from "$lib/DestinationForm.svelte";

    /** @type {import('./$types').PageData} */
    export let data;
</script>

<DestinationForm {...data} />
