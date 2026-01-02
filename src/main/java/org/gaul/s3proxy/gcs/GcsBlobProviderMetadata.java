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

import com.google.auto.service.AutoService;

import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.internal.BaseProviderMetadata;

/**
 * Implementation of org.jclouds.types.ProviderMetadata for Google Cloud
 * Storage using the native SDK (not jclouds).
 *
 * <p>This provider uses the Google Cloud Storage Java SDK directly,
 * bypassing jclouds' GCS implementation. This is useful for features
 * not well-supported by jclouds, such as uniform bucket-level access.</p>
 */
@AutoService(ProviderMetadata.class)
public final class GcsBlobProviderMetadata extends BaseProviderMetadata {
    public GcsBlobProviderMetadata() {
        super(builder());
    }

    public GcsBlobProviderMetadata(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return builder().fromProviderMetadata(this);
    }

    public static Properties defaultProperties() {
        var properties = new Properties();
        return properties;
    }

    public static final class Builder extends BaseProviderMetadata.Builder {
        protected Builder() {
            id("google-cloud-storage-sdk")
                .name("Google Cloud Storage (SDK)")
                .apiMetadata(new GcsBlobApiMetadata())
                .endpoint("https://storage.googleapis.com")
                .homepage(URI.create("https://cloud.google.com/storage"))
                .console(URI.create("https://console.cloud.google.com/storage"))
                .linkedServices("google-cloud-storage")
                .iso3166Codes("US", "EU", "ASIA")
                .defaultProperties(
                        GcsBlobProviderMetadata.defaultProperties());
        }

        @Override
        public GcsBlobProviderMetadata build() {
            return new GcsBlobProviderMetadata(this);
        }

        @Override
        public Builder fromProviderMetadata(
                ProviderMetadata in) {
            super.fromProviderMetadata(in);
            return this;
        }
    }
}
