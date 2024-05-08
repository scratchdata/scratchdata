<script>
    // @ts-nocheck
    /** @type {import('./$types').PageData} */

    import { fade } from "svelte/transition";
    import { notify } from "../../../lib/index"

    export let data;

    let visible = true;
    let destinationId;

    async function getDestinations() {
        // /api/destinations
        // await new Promise((r) => setTimeout(r, 500));
        // return "hello";

        return await (await fetch('/api/destinations?api_key=local')).json()
    }

    async function deleteDestination(id) {
        try {
            const response = await fetch(`/api/destinations/${id}?api_key=local`, {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                notify('Failed to delete destination', 'danger');
            }
    
            notify('Destination deleted successfully', 'success');
        } catch (error) {
            notify('Error deleting destination', 'danger');
        }
    }

    let destinations = getDestinations();

    function confirmDelete(id) {
        const dialog = document.querySelector('.delete-dialog');
        destinationId = id;

        if(dialog) {
            dialog.show();
        }
    }

    function deleteConfirmed() {
        const dialog = document.querySelector('.delete-dialog');
        deleteDestination(destinationId);

        if(dialog) {
            dialog.hide();
        }
    }

    function deleteCanceled() {
        const dialog = document.querySelector('.delete-dialog');
        
        if(dialog) {
            dialog.hide();
        }
    }
</script>

<h2>Connections</h2>

<p>
    <sl-button href="/dashboard/connections/new">Add Destination</sl-button>
</p>

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
            {dest.ID} - {dest.Type} - {dest.Name} <button on:click={() => confirmDelete(dest.ID)}>delete</button>
            <br>
            {/each}
            <!-- done  -->
        <!-- </p> -->
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}

<sl-dialog class="delete-dialog" label="Delete Confirmation" aria-labelledby="delete-confirmation" aria-describedby="delete-confirmation-message">
    <p id="delete-confirmation-message">Are you sure you want to delete this destination?</p>
    <sl-button slot="footer" variant="primary" on:click={deleteConfirmed}>Yes</sl-button>
    <sl-button slot="footer" variant="default" on:click={deleteCanceled}>No</sl-button>
</sl-dialog>
