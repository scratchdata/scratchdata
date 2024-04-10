package util

import (
	"reflect"
	"strings"
)

type Form struct {
	Name    string
	Type    string
	Label   string
	Default string
}

// TODO breadchris return error if form tag is malformed
func ConvertToForms(server any) []Form {
	var forms []Form

	serverValue := reflect.ValueOf(server)
	serverType := serverValue.Type()

	for i := 0; i < serverValue.NumField(); i++ {
		fieldType := serverType.Field(i)

		form := fieldType.Tag.Get("form")
		schema := fieldType.Tag.Get("schema")

		parts := strings.Split(schema, ",")
		schemaName := parts[0]

		parts = strings.Split(form, ",")
		formLabel := ""
		formDefault := ""
		formType := ""
		for _, part := range parts {
			if strings.HasPrefix(part, "label:") {
				formLabel = strings.TrimPrefix(part, "label:")
			}
			if strings.HasPrefix(part, "default:") {
				formDefault = strings.TrimPrefix(part, "default:")
			}
			if strings.HasPrefix(part, "type:") {
				formType = strings.TrimPrefix(part, "type:")
			}
		}

		if formType != "" {
			forms = append(forms, Form{
				Name:    schemaName,
				Type:    formType,
				Label:   formLabel,
				Default: formDefault,
			})
		}
	}
	return forms
}
