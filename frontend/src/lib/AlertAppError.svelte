<script>
    import { onMount } from "svelte";

    let alertElement = null;

    let errorMessage = "";

    export let open = false;
    export let closable = true;
    export let variant = "danger";
    export let duration = Infinity;
    export let iconName = "exclamation-octagon";
    export let ready = false;

    export function show(error) {
        errorMessage = error;
        alertElement?.show();
    }

    export function toast(error) {
        errorMessage = error;
        alertElement?.toast();
    }

    export function hide() {
        alertElement?.hide();
    }

    let props = { closable, variant, duration };

    if (open) {
        props.open = open;
    }

    onMount(async () => {
        await customElements.whenDefined("sl-alert");
        if (!alertElement && typeof document !== "undefined") {
            alertElement = document.querySelector("sl-alert.alert-app-error");
        }
        ready = true;
    });
</script>

<sl-alert bind:this="{alertElement}" class="alert-app-error" {...props}>
    <sl-icon slot="icon" name="{iconName}"></sl-icon>
    <pre>{errorMessage}</pre>
</sl-alert>
