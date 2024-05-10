<script>
    import { goto } from "$app/navigation";
    import AlertAppError from "$lib/AlertAppError.svelte";
    import DestinationBoolParam from "$lib/DestinationBoolParam.svelte";
    import DestinationNumberParam from "$lib/DestinationNumberParam.svelte";
    import DestinationPasswordParam from "$lib/DestinationPasswordParam.svelte";
    import DestinationTextareaParam from "$lib/DestinationTextareaParam.svelte";
    import DestinationTextParam from "$lib/DestinationTextParam.svelte";

    export let connectionType;
    export let parameters;

    let alertError;

    const parseFormValue = {
        number(value) {
            return typeof value === "number" ? value : parseInt(value, 10);
        },
        bool(value) {
            return typeof value === "boolean" ? value : value === "true";
        },
        text(value) {
            return value;
        },
        textarea(value) {
            return value;
        },
        password(value) {
            return value;
        },
    };

    function getFormValue(form, name, dataType) {
        const parse = parseFormValue[dataType];
        const { value } = form.querySelector(`[name="${name}"]`);

        return parse(value);
    }

    async function saveNewDestination() {
        const form = this;
        const payload = {
            type: connectionType,
            name: getFormValue(form, "name", "text"),
            settings: {}
        };

        for (const { name, type } of parameters.form_fields) {
            payload.settings[name] = getFormValue(form, name, type);
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
            const location = "/dashboard/connections";

            await goto(location);
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
