/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package protoenum parses and formats protobuf enums from their descriptors.
package protoenum

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Enum is a constraint for protobuf generated enum types.
type Enum interface {
	protoreflect.Enum
	~int32
}

// Name returns the user-facing spelling of a protobuf enum value.
func Name(value protoreflect.Enum) string {
	if value == nil || value.Descriptor() == nil {
		return ""
	}
	if valueDescriptor := value.Descriptor().Values().ByNumber(value.Number()); valueDescriptor != nil {
		return formatValue(value.Descriptor(), valueDescriptor)
	}
	return ""
}

// Parse parses text into an EnumNumber using descriptor.
// Exact match without case folding; empty text yields UNSPECIFIED.
func Parse(descriptor protoreflect.EnumDescriptor, text string) (protoreflect.EnumNumber, error) {
	if descriptor == nil {
		return 0, errors.New("descriptor is nil")
	}
	var unspecifiedNumber protoreflect.EnumNumber
	var accepted []string
	for i, values := 0, descriptor.Values(); i < values.Len(); i++ {
		valueDescriptor := values.Get(i)
		derived := formatValue(descriptor, valueDescriptor)
		if derived == "" {
			unspecifiedNumber = valueDescriptor.Number()
		} else {
			accepted = append(accepted, derived)
		}
		if text != "" && derived == text {
			return valueDescriptor.Number(), nil
		}
	}
	if text == "" {
		return unspecifiedNumber, nil
	}
	return unspecifiedNumber, fmt.Errorf("unknown %s %q (accepted values: %s)", typeName(descriptor), text, strings.Join(accepted, ", "))
}

// Value wraps a target protobuf enum to implement flag.Value and pflag.Value.
type Value[E Enum] struct{ target *E }

// NewValue returns a Value that sets target when parsed.
func NewValue[E Enum](target *E) *Value[E] { return &Value[E]{target: target} }

// String returns the user-facing spelling of the target enum.
func (v *Value[E]) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return Name(*v.target)
}

// Set parses text and sets target to the parsed enum value.
func (v *Value[E]) Set(text string) error {
	if v == nil || v.target == nil {
		return errors.New("target is nil")
	}
	number, err := Parse((*v.target).Descriptor(), text)
	if err == nil {
		*v.target = E(number)
	}
	return err
}

// Type returns the lowercase-hyphenated enum name.
func (v *Value[E]) Type() string {
	var zero E
	if v != nil && v.target != nil {
		zero = *v.target
	}
	return typeName(zero.Descriptor())
}

func formatValue(descriptor protoreflect.EnumDescriptor, valueDescriptor protoreflect.EnumValueDescriptor) string {
	raw := strings.TrimPrefix(string(valueDescriptor.Name()), upperSnake(string(descriptor.Name()))+"_")
	if raw == "UNSPECIFIED" {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(raw, "_", "-"))
}

func typeName(descriptor protoreflect.EnumDescriptor) string {
	if descriptor == nil {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(upperSnake(string(descriptor.Name())), "_", "-"))
}

func upperSnake(name string) string {
	var builder strings.Builder
	for i, character := range name {
		if unicode.IsUpper(character) && i > 0 && name[i-1] != '_' && (!unicode.IsUpper(rune(name[i-1])) || (i+1 < len(name) && unicode.IsLower(rune(name[i+1])))) {
			builder.WriteByte('_')
		}
		builder.WriteRune(unicode.ToUpper(character))
	}
	return builder.String()
}
