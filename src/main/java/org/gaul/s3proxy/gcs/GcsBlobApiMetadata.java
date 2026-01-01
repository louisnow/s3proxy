/*
 * Copyright 2014-2025 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy.gcs;

import java.net.URI;
import java.util.Properties;
import java.util.Set;

import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.reference.BlobStoreConstants;
import org.jclouds.reflect.Reflection2;
import org.jclouds.rest.internal.BaseHttpApiMetadata;


@SuppressWarnings("rawtypes")
public final class GcsBlobApiMetadata extends BaseHttpApiMetadata {
    public static final String GCS_PROJECT_ID = "gcs.project-id";
    public static final String GCS_CLIENT_EMAIL = "gcs.client-email";

    public GcsBlobApiMetadata() {
        this(builder());
    }

    protected GcsBlobApiMetadata(Builder builder) {
        super(builder);
    }

    private static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder().fromApiMetadata(this);
    }

    public static Properties defaultProperties() {
        Properties properties = BaseHttpApiMetadata.defaultProperties();
        properties.setProperty(BlobStoreConstants.PROPERTY_USER_METADATA_PREFIX,
                "x-goog-meta-");
        properties.setProperty(GCS_PROJECT_ID, "");
        properties.setProperty(GCS_CLIENT_EMAIL, "");
        return properties;
    }

    private interface GcsBlobClient {
    }

    public static final class Builder
            extends BaseHttpApiMetadata.Builder<GcsBlobClient, Builder> {
        protected Builder() {
            super(GcsBlobClient.class);
            id("gcs-sdk")
                .name("Google Cloud Storage API")
                .identityName("Client Email or Access Key")
                .credentialName("Private Key or Secret Key")
                .version("v1")
                .defaultEndpoint("https://storage.googleapis.com")
                .documentation(URI.create(
                        "https://cloud.google.com/storage/docs"))
                .defaultProperties(GcsBlobApiMetadata.defaultProperties())
                .view(Reflection2.typeToken(BlobStoreContext.class))
                .defaultModules(Set.of(GcsBlobStoreContextModule.class));
        }

        @Override
        public GcsBlobApiMetadata build() {
            return new GcsBlobApiMetadata(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
