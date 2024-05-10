<script>
    export let name;
    export let label;
    export let defaultValue;

    let props = {
        name,
        placeholder: label,
    };

    if (typeof defaultValue === "number") {
        props.defaultValue = defaultValue;
        props.value = defaultValue;
    } else if (typeof defaultValue === "string") {
        const v = parseInt(defaultValue, 10);

        if (!isNaN(v)) {
            props.defaultValue = v;
            props.value = v;
        }
    }
</script>

<sl-input type="number" pill {...props}>
    <label slot="label">{label}</label>
</sl-input>
