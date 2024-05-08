<script>
    // @ts-nocheck
    import { onMount } from 'svelte';
    import { page } from '$app/stores';
    import { notify } from '../../../../../lib/index';

    let formFields = [];
    let destinationName = '';

    onMount(async () => {
        const type = $page.params.type;
        const response = await fetch(`/api/destinations/params/${type}?api_key=local`);
        const data = await response.json();
        formFields = data.form_fields;

        formFields.forEach(field => {
            if(field.type === "bool") {
                field.checked = field.default;
            }
            else {
                field.value = field.default;
            }
        });
    });

    async function saveForm() {
        const formData = {
            name: destinationName,
            settings: {}
        };

        formFields.forEach(field => {
            formData.settings[field.name] = field.value;
            if (field.type === 'bool') {
                formData.settings[field.name] = field.checked;
            }
        });

        try {
            const type = $page.params.type;
            const response = await fetch('/api/destinations?api_key=local', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    type,
                    ...formData
                })
            });

            if (response.ok) {
                window.location.href = '/dashboard/connections';
            } else {
                const errorData = await response.json();
                notify(errorData.error, "danger");
            }
        } catch (error) {
            notify("An unexpected error occurred.", "danger");
        }
    }
</script>

<h2>Add New Connection</h2>

{#if formFields.length > 0}
    <form on:submit|preventDefault={saveForm}>
        {#each formFields as field}
            <label>
                {field.label}
                {#if field.type === 'textarea'}
                    <textarea name={field.name} bind:value={field.value}></textarea>
                {:else if field.type === 'bool'}
                    <input type="checkbox" name={field.name} bind:checked={field.checked} />
                {:else}
                    {#if field.type === 'password'}
                        <input type="password" name={field.name} bind:value={field.value} />
                    {:else if field.type === 'number'}
                        <input type="number" name={field.name} bind:value={field.value} />
                    {:else}
                        <input type="text" name={field.name} bind:value={field.value} />
                    {/if}
                {/if}
            </label>
            <br>
        {/each}

        <label>
            Destination Name
            <input type="text" bind:value={destinationName} />
        </label>

        <br>
        <button type="submit">Save</button>
    </form>
{:else}
    <p>Loading form...</p>
{/if}
