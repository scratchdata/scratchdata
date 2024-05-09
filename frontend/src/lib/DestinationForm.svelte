<script>
    import { redirect } from "@sveltejs/kit";
    import DestinationBoolParam from "$lib/DestinationBoolParam.svelte";
    import DestinationNumberParam from "$lib/DestinationNumberParam.svelte";
    import DestinationPasswordParam from "$lib/DestinationPasswordParam.svelte";
    import DestinationTextareaParam from "$lib/DestinationTextareaParam.svelte";
    import DestinationTextParam from "$lib/DestinationTextParam.svelte";

    async function getDestinationParameters(connectionType) {
        const resource = `/api/destinations/params/${connectionType}`;
        const options = {};
        const response = await fetch(resource, options);
        const parameters = await response.json();

        return parameters;
    }

    async function emptyParams() {
        return {
            form_fields: [],
        };
    }

    function getFormValue(form, name) {
        return form.querySelector(`[name="${name}"]`).value;
    }

    async function saveNewDestination() {
        const form = this;
        const payload = {
            type: connectionType,
            name: getFormValue(form, "name"),
            settings: {}
        };

        const parameters = await connectionParamsResponse;

        for (const { name } of parameters.form_fields) {
            payload.settings[name] = getFormValue(form, name);
        }

        const resource = "/api/destinations";
        const body = JSON.stringify(payload);
        const options = {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "content-length": body.length,
            },
            body
        };

        const response = await fetch(resource, options);

        if (response.ok) {
            const statusSeeOther = 303;
            const location = "/dashboard/connections";
            redirect(statusSeeOther, location);
        } else {
            const errorText = await response.text();

            alertError.show(errorText);
        }
    }

    let connectionParamsResponse = emptyParams();

    export let alertError;
    export let connectionType;
    export let loaded = false;
    export function load(name) {
        if (name && name !== connectionType) {
            loaded = false;
            connectionType = name;
        }

        if (loaded === true) {
            return;
        }

        connectionParamsResponse = getDestinationParameters(connectionType);
        connectionParamsResponse.then(() => {
            loaded = true;
        });
    }
</script>

{#await connectionParamsResponse}
{:then parameters}
    <form id="{connectionType}-form" on:submit|preventDefault="{saveNewDestination}">
        <DestinationTextParam name="name" label="Connection Name" />
        {#each parameters.form_fields as formField}
            {#if formField.type === "text"}
                <DestinationTextParam name="{formField.name}" label="{formField.label}" defaultValue="{formField.default}" />
            {:else if formField.type === "password"}
                <DestinationPasswordParam name="{formField.name}" label="{formField.label}" defaultValue="{formField.default}" />
            {:else if formField.type === "bool"}
                <DestinationBoolParam name="{formField.name}" label="{formField.label}" defaultValue="{formField.default}" />
            {:else if formField.type === "number"}
                <DestinationNumberParam name="{formField.name}" label="{formField.label}" defaultValue="{formField.default}" />
            {:else if formField.type === "textarea"}
                <DestinationTextareaParam name="{formField.name}" label="{formField.label}" defaultValue="{formField.default}" />
            {/if}
        {/each}
        <br>
        <sl-button type="submit" variant="primary" outline pill>
            <sl-icon name="cloud-plus" slot="prefix"></sl-icon>
            Save New Connection
        </sl-button>
        <sl-button type="reset" variant="warning" outline pill>
            <sl-icon name="arrow-counterclockwise" slot="prefix"></sl-icon>
            Reset
        </sl-button>
    </form>
{:catch err}
    <sl-alert variant="danger" open>
        <sl-icon slot="icon" name="exclamation-octagon"></sl-icon>
        <p>{err}</p>
    </sl-alert>
{/await}
