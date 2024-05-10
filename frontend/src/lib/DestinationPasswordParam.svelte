<script>
    export let name;
    export let label;
    export let defaultValue;

    let props = {
        name,
        placeholder: label,
    };

    if (typeof defaultValue === "string" && defaultValue.length > 0) {
        props.defaultValue = defaultValue;
        props.value = defaultValue;
    }
</script>

<sl-input type="password" pill passwordToggle {...props}>
    <sl-icon name="key" slot="prefix"></sl-icon>
    <label slot="label">{label}</label>
</sl-input>
