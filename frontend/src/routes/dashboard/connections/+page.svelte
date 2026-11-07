<script>
    /** @type {import('./$types').PageData} */

    import { onMount } from "svelte";

    export let data;

    let visible = true;

    async function getDestinations() {
        const response = await fetch("/api/destinations");
        if (!response.ok) {
            throw new Error(await response.text());
        }
        return await response.json();
    }

    let destinations = getDestinations();

    async function sendDeleteConnection(connectionId) {
        const resource = `/api/destinations/${connectionId}`;
        const options = {
            method: "DELETE",
        };
        const response = await fetch(resource, options);

        if (response.ok) {
            return { status: "success" };
        } else {
            const errorMessage = await response.text();

            return { status: "error", errorMessage };
        }
    }

    let context = {};

    function showDeleteDialog(destItem) {
        const dest = destItem && destItem.ID ? destItem : this?.value;
        if (!dest || !context.dialog) {
            return;
        }

        context.dialog.querySelector(".connection-name").innerText = dest.Name;
        context.alertSuccess.querySelector(".connection-name").innerText = dest.Name;
        context.alertError.querySelector(".connection-name").innerText = dest.Name;

        context.connectionId = dest.ID;
        context.dialog.show();
    }

    function hideDeleteDialog() {
        if (context.dialog) {
            context.dialog.hide();
        }
    }

    function showErrorAlert(errorText) {
        const { alertError } = context;
        if (alertError) {
            alertError.querySelector(".alert-error-text").innerText = errorText;
            alertError.show();
        }
    }

    function confirmDelete() {
        sendDeleteConnection(context.connectionId)
            .then(({ status, errorMessage }) => {
                if (status === "error") {
                    showErrorAlert(errorMessage);
                } else {
                    destinations = getDestinations();
                    context.alertSuccess.show();
                }
            })
            .catch(showErrorAlert)
            .finally(() => hideDeleteDialog());
    }

    onMount(() => {
        context.dialog = document.querySelector(".dialog-connection-delete");
        context.alertSuccess = document.querySelector(".alert-success.alert-connection-delete");
        context.alertError = document.querySelector(".alert-error.alert-connection-delete");
    });
</script>

<sl-dialog label="Delete Connection" class="dialog-connection-delete">
    Are you sure you want to delete <span class="connection-name"></span> connection?
    <sl-button slot="footer" variant="danger" size="medium" pill class="dialog-confirm" on:click="{confirmDelete}">Confirm</sl-button>
    <sl-button slot="footer" variant="default" size="medium" pill class="dialog-cancel" on:click="{hideDeleteDialog}">Cancel</sl-button>
</sl-dialog>

<sl-alert class="alert-success alert-connection-delete" duration="7000">
    <sl-icon slot="icon" name="info-circle"></sl-icon>
    Successfully removed <span class="connection-name"></span> connection.
</sl-alert>

<sl-alert class="alert-error alert-connection-delete" closable>
    <sl-icon slot="icon" name="exclamation-circle"></sl-icon>
    Unable to delete <span class="connection-name"></span> connection.
    <div class="alert-error-text"></div>
</sl-alert>

<h2>Connections</h2>

<p>
    <sl-button href="/dashboard/connections/new" variant="primary" outline pill>
        <sl-icon slot="prefix" name="plus-circle"></sl-icon>
        Add Destination
    </sl-button>
</p>

{#await destinations}
    <sl-spinner></sl-spinner>
{:then destList}
    {#if visible}
        {#if Array.isArray(destList) && destList.length > 0}
            {#each destList as dest (dest.ID)}
                {dest.ID} - {dest.Type} - {dest.Name}
                <sl-button
                    variant="danger"
                    size="small"
                    outline
                    pill
                    value="{dest}"
                    class="button-connection-delete"
                    on:click="{() => showDeleteDialog(dest)}"
                >
                    <sl-icon slot="prefix" name="trash"></sl-icon>
                    Delete
                </sl-button>
                <br>
            {/each}
        {:else}
            <p>No database connections configured yet.</p>
        {/if}
    {/if}
{:catch e}
    <p>err {e}</p>
{/await}
