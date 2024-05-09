<script lang="ts">
    /** @type {import('./$types').PageData} */

    import { onMount } from "svelte";
    import AlertAppError from "$lib/AlertAppError.svelte";
    import DestinationForm from "$lib/DestinationForm.svelte";

    let activeTab;
    let tabForms = {};
    let alertAppError;

    function onTabShow(event) {
        const connectionType = event.detail.name;
        const destinationForm = tabForms[connectionType];

        activeTab = connectionType;
        destinationForm.load(connectionType);
    }

    onMount(async () => {
        await Promise.allSettled([
            customElements.whenDefined("sl-tab-group"),
            customElements.whenDefined("sl-tab-panel"),
            customElements.whenDefined("sl-tab"),
        ]);

        const tabGroup = document.querySelector("sl-tab-group");
        const activeTabElement = tabGroup.querySelector("sl-tab[active]");

        tabGroup.addEventListener("sl-tab-show", onTabShow);

        if (!activeTab && activeTabElement) {
            const event = {
                detail: {
                    name: activeTabElement.panel,
                },
            };
            onTabShow(event);
        }
    });
</script>

<h2>New Connection</h2>

<AlertAppError bind:this="{alertAppError}" />

<sl-tab-group>
    <sl-tab slot="nav" panel="bigquery">BigQuery</sl-tab>
    <sl-tab slot="nav" panel="clickhouse">ClickHouse</sl-tab>
    <sl-tab slot="nav" panel="duckdb" active>DuckDB</sl-tab>
    <sl-tab slot="nav" panel="postgres">Postgres</sl-tab>
    <sl-tab slot="nav" panel="redshift">RedShift</sl-tab>

    <sl-tab-panel name="bigquery">
        <DestinationForm connectionType="bigquery" bind:this="{tabForms.bigquery}" bind:alertError="{alertAppError}"></DestinationForm>
    </sl-tab-panel>

    <sl-tab-panel name="clickhouse">
        <DestinationForm connectionType="clickhouse" bind:this="{tabForms.clickhouse}" bind:alertError="{alertAppError}"></DestinationForm>
    </sl-tab-panel>

    <sl-tab-panel name="duckdb">
        <DestinationForm connectionType="duckdb" bind:this="{tabForms.duckdb}" bind:alertError="{alertAppError}"></DestinationForm>
    </sl-tab-panel>

    <sl-tab-panel name="postgres">
        <DestinationForm connectionType="postgres" bind:this="{tabForms.postgres}" bind:alertError="{alertAppError}"></DestinationForm>
    </sl-tab-panel>

    <sl-tab-panel name="redshift">
        <DestinationForm connectionType="redshift" bind:this="{tabForms.redshift}" bind:alertError="{alertAppError}"></DestinationForm>
    </sl-tab-panel>
</sl-tab-group>
