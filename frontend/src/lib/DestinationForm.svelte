<script>
    import { redirect } from "@sveltejs/kit";
    import AlertAppError from "$lib/AlertAppError.svelte";
    import DestinationBoolParam from "$lib/DestinationBoolParam.svelte";
    import DestinationNumberParam from "$lib/DestinationNumberParam.svelte";
    import DestinationPasswordParam from "$lib/DestinationPasswordParam.svelte";
    import DestinationTextareaParam from "$lib/DestinationTextareaParam.svelte";
    import DestinationTextParam from "$lib/DestinationTextParam.svelte";

    export let connectionType;
    export let parameters;

    let alertError;

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
</script>

<h2>New {parameters.type} Connection</h2>

<AlertAppError bind:this="{alertError}" />

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
