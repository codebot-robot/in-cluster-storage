# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build context is the fakes3/ module directory.
FROM golang:1.27.2 AS builder

WORKDIR /app
COPY . .
# The binary only depends on the standard library; the module's other
# requirements are test-only and are not downloaded for this build.
RUN CGO_ENABLED=0 go build -o fakes3 ./cmd/fakes3

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/fakes3 /usr/local/bin/fakes3

EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/fakes3"]
