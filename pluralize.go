package godb

import "strings"

func Pluralize(name string) string {
	if strings.HasSuffix(name, "s") || strings.HasSuffix(name, "z") || strings.HasSuffix(name, "x") {
		return name + "es"
	} else if strings.HasSuffix(name, "y") {
		return name[:len(name)-1] + "ies"
	} else {
		return name + "s"
	}
}
