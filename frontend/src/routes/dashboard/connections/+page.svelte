<script>
    /** @type {import('./$types').PageData} */

    import { onMount } from "svelte";
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

    async function sendDeleteConnection(connectionId) {
        const resource = `/api/destinations/${connectionId}`;
        const options = {
            method: 'DELETE',
        };
        const response = await fetch(resource, options);

        if (response.ok) {
            return {status: "success"};
        } else {
            const errorMessage = await response.text();

            return {status: "error", errorMessage};
        }
    }

    let context = {};

    function showDeleteDialog() {
        const id = this.value;
        const buttonConfirm = context.dialog.querySelector('.dialog-confirm');
        
        context.dialog.querySelector('.connection-id').innerText = id.toString();
        context.alertSuccess.querySelector('.connection-id').innerText = id.toString();
        context.alertError.querySelector('.connection-id').innerText = id.toString();

        context.connectionId = id;
        context.dialog.show();
    }

    function showErrorAlert(errorText) {
        const {alertError} = context;
        alertError.querySelector('.alert-error-text').innerText = errorText;
        alertError.show();
    }

    function confirmDelete() {
        sendDeleteConnection(context.connectionId)
            .then(({status, errorMessage}) => {
                if (status === "error") {
                    showErrorAlert(errorMessage);
                } else {
                    destinations = getDestinations();
                    context.alertSuccess.show();
                }
            })
            .catch(showErrorAlert)
            .finally(() => context.dialog.hide());
    }

    onMount(() => {
        context.dialog = document.querySelector('.dialog-connection-delete');
        context.alertSuccess = document.querySelector('.alert-success.alert-connection-delete');
        context.alertError = document.querySelector('.alert-error.alert-connection-delete');
    });
</script>

<sl-dialog label="Delete Connection" class="dialog-connection-delete">
    Are you sure you want to delete <span class="connection-id"></span> connection?
    <sl-button slot="footer" variant="danger" size="medium" pill class="dialog-confirm" on:click="{confirmDelete}">Confirm</sl-button>
    <sl-button slot="footer" variant="default" size="medium" pill class="dialog-cancel" on:click="{context.dialog.hide()}">Cancel</sl-button>
</sl-dialog>

<sl-alert class="alert-success alert-connection-delete" duration="7000">
    <sl-icon slot="icon" name="info-circle"></sl-icon>
    Successfully removed <span class="connection-id"></span> connection.
</sl-alert>

<sl-alert class="alert-error alert-connection-delete" closable>
    <sl-icon slot="icon" name="exclamation-circle"></sl-icon>
    Unable to delete <span class="connection-id"></span> connection.
    <div class="alert-error-text"></div>
</sl-alert>

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
            {dest.ID} - {dest.Type} - {dest.Name} <sl-button variant="danger" size="small" outline pill value="{dest.ID}" class="button-connection-delete" on:click={showDeleteDialog}>
                <sl-icon slot="prefix" name="trash"></sl-icon>
                Delete
            </sl-button>
            <br>
            {/each}
            <!-- done  -->
        <!-- </p> -->
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}
