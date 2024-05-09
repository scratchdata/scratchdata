<script>
    export let name;
    export let label;
    export let defaultValue;

    let props = {
        name,
    };

    if (typeof defaultValue === "boolean") {
        props.defaultChecked = defaultValue;
        props.checked = defaultValue;
    } else if (defaultValue === "true") {
        props.defaultChecked = true;
        props.checked = true;
    }
</script>

<sl-switch {...props}>{label}</sl-switch>
