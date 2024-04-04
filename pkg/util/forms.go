package util

import "reflect"

type Form struct {
	Name  string
	Type  string
	Label string
}

func ConvertToForms(server any) []Form {
	var forms []Form

	// Use reflection to iterate over the fields of DuckDBServer
	serverValue := reflect.ValueOf(server)
	serverType := serverValue.Type()

	for i := 0; i < serverValue.NumField(); i++ {
		// field := serverValue.Field(i)
		fieldType := serverType.Field(i)

		// Extract the form_type and form_label tags
		fieldName := fieldType.Tag.Get("mapstructure")
		formType := fieldType.Tag.Get("form_type")
		formLabel := fieldType.Tag.Get("form_label")

		// Create a Form instance and append it to the slice
		if formType != "" {
			forms = append(forms, Form{
				Name:  fieldName,
				Type:  formType,
				Label: formLabel,
			})
		}
	}

	return forms
}
