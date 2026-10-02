// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

import (
	"strings"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func makeTestTypeDef(
	name string,
	fields []*descriptorpb.FieldDescriptorProto,
	reservedRanges []*descriptorpb.DescriptorProto_ReservedRange,
	oneofs []*descriptorpb.OneofDescriptorProto,
	keyFields []int32,
	nestedMessages []*descriptorpb.DescriptorProto,
) *sdsv1.TypeDefinition {
	msgProto := &descriptorpb.DescriptorProto{
		Name:          proto.String(name),
		Field:         fields,
		ReservedRange: reservedRanges,
		OneofDecl:     oneofs,
		NestedType:    nestedMessages,
	}

	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileProto},
	}

	fp, _ := ComputeFingerprint(fds)

	return &sdsv1.TypeDefinition{
		Id:          16,
		Name:        "testpkg." + name,
		Fingerprint: fp,
		Descriptors: fds,
		KeyFields:   keyFields,
	}
}

func field(name string, number int32, fieldType descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   fieldType.Enum(),
		Label:  label.Enum(),
	}
}

func fieldInOneof(name string, number int32, fieldType descriptorpb.FieldDescriptorProto_Type, oneofIndex int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:       proto.String(name),
		Number:     proto.Int32(number),
		Type:       fieldType.Enum(),
		Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		OneofIndex: proto.Int32(oneofIndex),
	}
}

func messageField(name string, number int32, typeName string, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(typeName),
		Label:    label.Enum(),
	}
}

func reservedRange(start, end int32) *descriptorpb.DescriptorProto_ReservedRange {
	return &descriptorpb.DescriptorProto_ReservedRange{
		Start: proto.Int32(start),
		End:   proto.Int32(end), // protobuf range is [start, end)
	}
}

func TestCheckCompatibilityTable(t *testing.T) {
	baseFields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}

	baseDef := makeTestTypeDef("Order", baseFields, nil, nil, []int32{1}, nil)

	tests := []struct {
		name        string
		oldDef      *sdsv1.TypeDefinition
		newDef      *sdsv1.TypeDefinition
		expectValid bool
		errContains string
	}{
		{
			name:        "identical definition",
			oldDef:      baseDef,
			newDef:      baseDef,
			expectValid: true,
		},
		{
			name:   "add new field (compatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("notes", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: true,
		},
		{
			name:   "rename field (compatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("order_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("buyer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("amount", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: true,
		},
		{
			name:   "remove field with reserved number (compatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, []*descriptorpb.DescriptorProto_ReservedRange{reservedRange(3, 4)}, nil, []int32{1}, nil),
			expectValid: true,
		},
		{
			name:   "remove field WITHOUT reserving number (incompatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "without reserving its field number",
		},
		{
			name:   "change field type from double to string (incompatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "changed kind",
		},
		{
			name:   "change field number (incompatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("total", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "without reserving its field number",
		},
		{
			name:   "change cardinality from optional to repeated (incompatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
				field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "changed cardinality",
		},
		{
			name:        "change key_fields (incompatible)",
			oldDef:      baseDef,
			newDef:      makeTestTypeDef("Order", baseFields, nil, nil, []int32{1, 2}, nil),
			expectValid: false,
			errContains: "key_fields mismatch",
		},
		{
			name:   "move field into oneof (incompatible)",
			oldDef: baseDef,
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				fieldInOneof("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, 0),
				field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, []*descriptorpb.OneofDescriptorProto{{Name: proto.String("cust_oneof")}}, []int32{1}, nil),
			expectValid: false,
			errContains: "oneof membership changed",
		},
		{
			name: "reuse previously reserved field number for new field (incompatible)",
			oldDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, []*descriptorpb.DescriptorProto_ReservedRange{reservedRange(4, 5)}, nil, []int32{1}, nil),
			newDef: makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				field("reused", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "reuses a field number reserved in older schema",
		},
		{
			name:        "message name mismatch (incompatible)",
			oldDef:      baseDef,
			newDef:      makeTestTypeDef("Invoice", baseFields, nil, nil, []int32{1}, nil),
			expectValid: false,
			errContains: "message name mismatch",
		},
		{
			name: "nested message compatible evolution",
			oldDef: makeTestTypeDef("Container", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				messageField("item", 2, "testpkg.Container.Item", descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, []*descriptorpb.DescriptorProto{
				{
					Name: proto.String("Item"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("code", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
					},
				},
			}),
			newDef: makeTestTypeDef("Container", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				messageField("item", 2, "testpkg.Container.Item", descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, []*descriptorpb.DescriptorProto{
				{
					Name: proto.String("Item"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("code", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
						field("qty", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
					},
				},
			}),
			expectValid: true,
		},
		{
			name: "nested message incompatible evolution (changed kind)",
			oldDef: makeTestTypeDef("Container", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				messageField("item", 2, "testpkg.Container.Item", descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, []*descriptorpb.DescriptorProto{
				{
					Name: proto.String("Item"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("code", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
					},
				},
			}),
			newDef: makeTestTypeDef("Container", []*descriptorpb.FieldDescriptorProto{
				field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				messageField("item", 2, "testpkg.Container.Item", descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			}, nil, nil, []int32{1}, []*descriptorpb.DescriptorProto{
				{
					Name: proto.String("Item"),
					Field: []*descriptorpb.FieldDescriptorProto{
						field("code", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
					},
				},
			}),
			expectValid: false,
			errContains: "changed kind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckCompatibility(tt.oldDef, tt.newDef)
			if tt.expectValid {
				if err != nil {
					t.Fatalf("unexpected compatibility error: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected compatibility error, got nil")
				}
				if tt.errContains != "" && !stringContains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain expected substring %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func stringContains(s, substr string) bool {
	return strings.Contains(s, substr)
}
