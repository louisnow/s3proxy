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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response.Status;

import org.gaul.s3proxy.PutOptions2;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.Tier;
import org.jclouds.blobstore.domain.internal.BlobBuilderImpl;
import org.jclouds.blobstore.domain.internal.BlobMetadataImpl;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.domain.internal.StorageMetadataImpl;
import org.jclouds.blobstore.internal.BaseBlobStore;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.BlobUtils;
import org.jclouds.collect.Memoized;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.http.HttpCommand;
import org.jclouds.http.HttpRequest;
import org.jclouds.http.HttpResponse;
import org.jclouds.http.HttpResponseException;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.ContentMetadataBuilder;
import org.jclouds.io.Payload;
import org.jclouds.io.PayloadSlicer;
import org.jclouds.providers.ProviderMetadata;

@Singleton
public final class GcsBlobStore extends BaseBlobStore {
    private static final String STUB_BLOB_PREFIX = ".s3proxy-mpu/";
    private static final String MULTIPART_PREFIX = ".s3proxy-mpu-parts/";
    private static final String TARGET_BLOB_NAME_KEY = "s3proxy_target_blob_name";
    private static final String UPLOAD_ID_KEY = "s3proxy_upload_id";
    private static final HashFunction MD5 = Hashing.md5();

    // GCS compose has a limit of 32 source objects per operation
    private static final int GCS_COMPOSE_LIMIT = 32;
    // Maximum number of parts supported (can compose in stages)
    private static final int MAX_PARTS = 10000;

    private final Storage storage;
    private final String projectId;
    // Track container access (since GCS IAM is complex, use in-memory state)
    private final java.util.concurrent.ConcurrentHashMap<String, ContainerAccess>
            containerAccessMap = new java.util.concurrent.ConcurrentHashMap<>();

    @Inject
    GcsBlobStore(BlobStoreContext context, BlobUtils blobUtils,
            Supplier<Location> defaultLocation,
            @Memoized Supplier<Set<? extends Location>> locations,
            PayloadSlicer slicer,
            @org.jclouds.location.Provider Supplier<Credentials> creds,
            ProviderMetadata provider) {
        super(context, blobUtils, defaultLocation, locations, slicer);
        var credential = creds.get();
        this.projectId = credential.identity;
        String endpoint = provider.getEndpoint();

        StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder()
                .setProjectId(projectId);

        // Configure custom endpoint (for testing with fake-gcs-server)
        if (endpoint != null && !endpoint.isEmpty()) {
            optionsBuilder.setHost(endpoint);
        }

        // Configure authentication
        if (credential.credential != null && !credential.credential.isEmpty()) {
            // Credential should be JSON content (Main.java reads file if path)
            try {
                GoogleCredentials credentials =
                        ServiceAccountCredentials.fromStream(
                                new ByteArrayInputStream(
                                        credential.credential.getBytes(
                                                StandardCharsets.UTF_8)));
                optionsBuilder.setCredentials(credentials);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to load GCS credentials", e);
            }
        } else if (endpoint != null && !endpoint.isEmpty()) {
            // Use NoCredentials for emulator/testing (e.g., fake-gcs-server)
            optionsBuilder.setCredentials(NoCredentials.getInstance());
        }
        // If no credential and no endpoint, use Application Default Credentials

        this.storage = optionsBuilder.build().getService();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        var set = ImmutableSet.<StorageMetadata>builder();
        for (Bucket bucket : storage.list().iterateAll()) {
            set.add(new StorageMetadataImpl(StorageType.CONTAINER, /*id=*/ null,
                    bucket.getName(), /*location=*/ null, /*uri=*/ null,
                    /*eTag=*/ null,
                    toDate(bucket.getCreateTimeOffsetDateTime()),
                    toDate(bucket.getUpdateTimeOffsetDateTime()),
                    Map.of(), /*size=*/ null,
                    Tier.STANDARD));
        }
        return new PageSetImpl<StorageMetadata>(set.build(), null);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container,
            ListContainerOptions options) {
        var gcsOptions = new ArrayList<BlobListOption>();

        if (options.getPrefix() != null) {
            gcsOptions.add(BlobListOption.prefix(options.getPrefix()));
        }
        if (options.getMaxResults() != null) {
            gcsOptions.add(BlobListOption.pageSize(options.getMaxResults()));
        }
        if (options.getMarker() != null) {
            gcsOptions.add(BlobListOption.pageToken(options.getMarker()));
        }
        if (options.getDelimiter() != null) {
            gcsOptions.add(BlobListOption.delimiter(options.getDelimiter()));
        } else if (!options.isRecursive()) {
            gcsOptions.add(BlobListOption.delimiter("/"));
        }

        var set = ImmutableSet.<StorageMetadata>builder();
        com.google.api.gax.paging.Page<Blob> page;
        try {
            page = storage.list(container,
                    gcsOptions.toArray(new BlobListOption[0]));
        } catch (StorageException se) {
            translateAndRethrowException(se, container, null);
            throw se;
        }

        for (Blob blob : page.getValues()) {
            if (blob.isDirectory()) {
                set.add(new StorageMetadataImpl(StorageType.RELATIVE_PATH,
                        /*id=*/ null, blob.getName(), /*location=*/ null,
                        /*uri=*/ null, /*eTag=*/ null,
                        /*creationDate=*/ null,
                        /*lastModified=*/ null,
                        Map.of(),
                        /*size=*/ null,
                        Tier.STANDARD));
            } else {
                set.add(new StorageMetadataImpl(StorageType.BLOB,
                        /*id=*/ null, blob.getName(), /*location=*/ null,
                        /*uri=*/ null, blob.getEtag(),
                        toDate(blob.getCreateTimeOffsetDateTime()),
                        toDate(blob.getUpdateTimeOffsetDateTime()),
                        Map.of(),
                        blob.getSize(),
                        toTier(blob.getStorageClass())));
            }
        }

        return new PageSetImpl<StorageMetadata>(set.build(),
                page.getNextPageToken());
    }

    @Override
    public boolean containerExists(String container) {
        try {
            Bucket bucket = storage.get(container);
            return bucket != null && bucket.exists();
        } catch (StorageException se) {
            if (se.getCode() == 404) {
                return false;
            }
            throw se;
        }
    }

    @Override
    public boolean createContainerInLocation(Location location,
            String container) {
        return createContainerInLocation(location, container,
                new CreateContainerOptions());
    }

    @Override
    public boolean createContainerInLocation(Location location,
            String container, CreateContainerOptions options) {
        try {
            BucketInfo.Builder bucketInfo = BucketInfo.newBuilder(container);
            if (location != null && location.getId() != null) {
                bucketInfo.setLocation(location.getId());
            }
            storage.create(bucketInfo.build());
            return true;
        } catch (StorageException se) {
            if (se.getCode() == 409) {
                // Bucket already exists
                return false;
            }
            translateAndRethrowException(se, container, null);
            throw se;
        }
    }

    @Override
    public void deleteContainer(String container) {
        try {
            // First, delete all blobs in the container
            var page = storage.list(container);
            for (Blob blob : page.iterateAll()) {
                storage.delete(blob.getBlobId());
            }
            // Then delete the container
            storage.delete(container);
        } catch (StorageException se) {
            if (se.getCode() != 404) {
                throw se;
            }
        }
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        try {
            // Check if container has any blobs
            var page = storage.list(container, BlobListOption.pageSize(1));
            if (page.getValues().iterator().hasNext()) {
                return false;
            }
            storage.delete(container);
            return true;
        } catch (StorageException se) {
            if (se.getCode() == 404) {
                return true;
            }
            throw se;
        }
    }

    @Override
    public boolean blobExists(String container, String key) {
        try {
            BlobId blobId = BlobId.of(container, key);
            Blob blob = storage.get(blobId);
            return blob != null && blob.exists();
        } catch (StorageException se) {
            if (se.getCode() == 404) {
                return false;
            }
            throw se;
        }
    }

    @Override
    public org.jclouds.blobstore.domain.Blob getBlob(String container,
            String key, GetOptions options) {
        BlobId blobId = BlobId.of(container, key);

        Blob gcsBlob;
        try {
            gcsBlob = storage.get(blobId);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, key);
            throw se;
        }

        if (gcsBlob == null) {
            throw new KeyNotFoundException(container, key, "Blob not found");
        }

        // Handle conditional GET (If-Match/If-None-Match) using ETag comparison
        // GCS uses generation numbers, not ETags, so we handle this manually
        String blobEtag = gcsBlob.getEtag();
        if (options.getIfMatch() != null) {
            String ifMatch = options.getIfMatch().replace("\"", "");
            if (blobEtag != null && !blobEtag.equals(ifMatch)) {
                // ETag doesn't match - precondition failed
                var request = HttpRequest.builder()
                        .method("GET")
                        .endpoint("https://storage.googleapis.com")
                        .build();
                var response = HttpResponse.builder()
                        .statusCode(Status.PRECONDITION_FAILED.getStatusCode())
                        .build();
                throw new HttpResponseException(
                        new HttpCommand(request), response,
                        "If-Match condition failed");
            }
        }
        if (options.getIfNoneMatch() != null) {
            String ifNoneMatch = options.getIfNoneMatch().replace("\"", "");
            if (blobEtag != null && blobEtag.equals(ifNoneMatch)) {
                // ETag matches - throw 304 Not Modified
                var request = HttpRequest.builder()
                        .method("GET")
                        .endpoint("https://storage.googleapis.com")
                        .build();
                var response = HttpResponse.builder()
                        .statusCode(Status.NOT_MODIFIED.getStatusCode())
                        .build();
                throw new HttpResponseException(
                        new HttpCommand(request), response,
                        "If-None-Match condition met");
            }
        }

        // Handle range requests
        Long rangeStart = null;
        Long rangeEnd = null;
        if (!options.getRanges().isEmpty()) {
            var rangeStr = options.getRanges().get(0);
            var ranges = rangeStr.split("-", 2);
            if (ranges[0].isEmpty()) {
                // Suffix range: -500 means last 500 bytes
                long suffix = Long.parseLong(ranges[1]);
                rangeStart = gcsBlob.getSize() - suffix;
                rangeEnd = gcsBlob.getSize() - 1;
            } else if (ranges.length == 1 || ranges[1].isEmpty()) {
                // Open-ended range: 500- means from byte 500 to end
                rangeStart = Long.parseLong(ranges[0]);
                rangeEnd = gcsBlob.getSize() - 1;
            } else {
                rangeStart = Long.parseLong(ranges[0]);
                rangeEnd = Long.parseLong(ranges[1]);
            }
        }

        InputStream inputStream;
        long contentLength;
        try {
            ReadChannel reader = gcsBlob.reader();
            if (rangeStart != null) {
                reader.seek(rangeStart);
                contentLength = rangeEnd - rangeStart + 1;
                inputStream = ByteStreams.limit(
                        Channels.newInputStream(reader), contentLength);
            } else {
                contentLength = gcsBlob.getSize();
                inputStream = Channels.newInputStream(reader);
            }
        } catch (StorageException se) {
            translateAndRethrowException(se, container, key);
            throw se;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to read blob", ioe);
        }

        var blob = new BlobBuilderImpl()
                .name(key)
                .userMetadata(gcsBlob.getMetadata() != null ?
                        gcsBlob.getMetadata() : Map.of())
                .payload(inputStream)
                .cacheControl(gcsBlob.getCacheControl())
                .contentDisposition(gcsBlob.getContentDisposition())
                .contentEncoding(gcsBlob.getContentEncoding())
                .contentLanguage(gcsBlob.getContentLanguage())
                .contentLength(contentLength)
                .contentType(gcsBlob.getContentType())
                .build();

        if (rangeStart != null) {
            blob.getAllHeaders().put(HttpHeaders.CONTENT_RANGE,
                    "bytes " + rangeStart + "-" + rangeEnd +
                            "/" + gcsBlob.getSize());
        }

        var metadata = blob.getMetadata();
        metadata.setETag(gcsBlob.getEtag());
        metadata.setCreationDate(toDate(gcsBlob.getCreateTimeOffsetDateTime()));
        metadata.setLastModified(toDate(gcsBlob.getUpdateTimeOffsetDateTime()));
        metadata.setSize(gcsBlob.getSize() != null ? gcsBlob.getSize() : 0L);
        return blob;
    }

    @Override
    public String putBlob(String container,
            org.jclouds.blobstore.domain.Blob blob) {
        return putBlob(container, blob, new PutOptions());
    }

    @Override
    public String putBlob(String container,
            org.jclouds.blobstore.domain.Blob blob, PutOptions options) {
        String key = blob.getMetadata().getName();
        BlobInfo.Builder blobInfo = BlobInfo.newBuilder(container, key);

        var contentMetadata = blob.getMetadata().getContentMetadata();
        if (contentMetadata.getCacheControl() != null) {
            blobInfo.setCacheControl(contentMetadata.getCacheControl());
        }
        if (contentMetadata.getContentDisposition() != null) {
            blobInfo.setContentDisposition(
                    contentMetadata.getContentDisposition());
        }
        if (contentMetadata.getContentEncoding() != null) {
            blobInfo.setContentEncoding(contentMetadata.getContentEncoding());
        }
        if (contentMetadata.getContentLanguage() != null) {
            blobInfo.setContentLanguage(contentMetadata.getContentLanguage());
        }
        if (contentMetadata.getContentType() != null) {
            blobInfo.setContentType(contentMetadata.getContentType());
        }
        if (contentMetadata.getContentMD5AsHashCode() != null) {
            blobInfo.setMd5(Base64.getEncoder().encodeToString(
                    contentMetadata.getContentMD5AsHashCode().asBytes()));
        }

        var userMetadata = blob.getMetadata().getUserMetadata();
        if (userMetadata != null && !userMetadata.isEmpty()) {
            blobInfo.setMetadata(userMetadata);
        }

        var tier = blob.getMetadata().getTier();
        if (tier != null && tier != Tier.STANDARD) {
            blobInfo.setStorageClass(toStorageClass(tier));
        }

        List<BlobTargetOption> targetOptions = new ArrayList<>();
        if (options instanceof PutOptions2) {
            var putOptions2 = (PutOptions2) options;
            if (putOptions2.getIfMatch() != null) {
                targetOptions.add(BlobTargetOption.generationMatch(
                        parseGeneration(putOptions2.getIfMatch())));
            }
            if (putOptions2.getIfNoneMatch() != null) {
                if ("*".equals(putOptions2.getIfNoneMatch())) {
                    targetOptions.add(BlobTargetOption.doesNotExist());
                } else {
                    targetOptions.add(BlobTargetOption.generationNotMatch(
                            parseGeneration(putOptions2.getIfNoneMatch())));
                }
            }
        }

        try (var is = blob.getPayload().openStream()) {
            byte[] content = is.readAllBytes();
            Blob created = storage.create(blobInfo.build(), content,
                    targetOptions.toArray(new BlobTargetOption[0]));
            return created.getEtag();
        } catch (StorageException se) {
            translateAndRethrowException(se, container, key);
            throw se;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to write blob", ioe);
        }
    }

    @Override
    public String copyBlob(String fromContainer, String fromName,
            String toContainer, String toName, CopyOptions options) {
        BlobId source = BlobId.of(fromContainer, fromName);
        BlobId target = BlobId.of(toContainer, toName);

        CopyRequest.Builder copyRequest = CopyRequest.newBuilder()
                .setSource(source)
                .setTarget(target);

        var contentMetadata = options.contentMetadata();
        var userMetadata = options.userMetadata();

        // Build target info with both content metadata and user metadata
        if (contentMetadata != null || userMetadata != null) {
            BlobInfo.Builder targetInfo = BlobInfo.newBuilder(target);

            if (contentMetadata != null) {
                if (contentMetadata.getCacheControl() != null) {
                    targetInfo.setCacheControl(
                            contentMetadata.getCacheControl());
                }
                if (contentMetadata.getContentDisposition() != null) {
                    targetInfo.setContentDisposition(
                            contentMetadata.getContentDisposition());
                }
                if (contentMetadata.getContentEncoding() != null) {
                    targetInfo.setContentEncoding(
                            contentMetadata.getContentEncoding());
                }
                if (contentMetadata.getContentLanguage() != null) {
                    targetInfo.setContentLanguage(
                            contentMetadata.getContentLanguage());
                }
                if (contentMetadata.getContentType() != null) {
                    targetInfo.setContentType(contentMetadata.getContentType());
                }
            }

            if (userMetadata != null) {
                targetInfo.setMetadata(userMetadata);
            }

            copyRequest.setTarget(targetInfo.build());
        }

        try {
            Blob result = storage.copy(copyRequest.build()).getResult();
            return result.getEtag();
        } catch (StorageException se) {
            translateAndRethrowException(se, fromContainer, fromName);
            throw se;
        }
    }

    @Override
    public void removeBlob(String container, String key) {
        try {
            storage.delete(BlobId.of(container, key));
        } catch (StorageException se) {
            if (se.getCode() != 404) {
                throw se;
            }
        }
    }

    @Override
    public BlobMetadata blobMetadata(String container, String key) {
        BlobId blobId = BlobId.of(container, key);
        Blob gcsBlob;
        try {
            gcsBlob = storage.get(blobId);
        } catch (StorageException se) {
            if (se.getCode() == 404) {
                return null;
            }
            translateAndRethrowException(se, container, null);
            throw se;
        }

        if (gcsBlob == null) {
            return null;
        }

        Long size = gcsBlob.getSize();
        return new BlobMetadataImpl(/*id=*/ null, key, /*location=*/ null,
                /*uri=*/ null, gcsBlob.getEtag(),
                toDate(gcsBlob.getCreateTimeOffsetDateTime()),
                toDate(gcsBlob.getUpdateTimeOffsetDateTime()),
                gcsBlob.getMetadata() != null ? gcsBlob.getMetadata() : Map.of(),
                /*publicUri=*/ null, container,
                toContentMetadata(gcsBlob),
                size != null ? size : 0L,
                toTier(gcsBlob.getStorageClass()));
    }

    @Override
    protected boolean deleteAndVerifyContainerGone(String container) {
        storage.delete(container);
        return true;
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        // GCS with uniform bucket-level access doesn't support
        // fine-grained ACLs - use in-memory tracking for S3 compatibility
        try {
            Bucket bucket = storage.get(container);
            if (bucket == null) {
                throw new ContainerNotFoundException(container, "");
            }
            return containerAccessMap.getOrDefault(container,
                    ContainerAccess.PRIVATE);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, null);
            throw se;
        }
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess access) {
        // GCS uniform bucket-level access uses IAM policies
        // For S3 compatibility, track access in memory
        // Note: This doesn't modify actual GCS IAM policies
        containerAccessMap.put(container, access);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String key) {
        // GCS with uniform bucket-level access doesn't have object-level ACLs
        return BlobAccess.PRIVATE;
    }

    @Override
    public void setBlobAccess(String container, String key, BlobAccess access) {
        // Not supported with uniform bucket-level access
        throw new UnsupportedOperationException(
                "Blob access control not supported with " +
                        "uniform bucket-level access");
    }

    // ========== Multipart Upload Implementation ==========

    @Override
    public MultipartUpload initiateMultipartUpload(String container,
            BlobMetadata blobMetadata, PutOptions options) {
        // Verify container exists
        if (!containerExists(container)) {
            throw new ContainerNotFoundException(container, "");
        }

        String uploadId = UUID.randomUUID().toString();
        String targetBlobName = blobMetadata.getName();
        String stubKey = STUB_BLOB_PREFIX + uploadId;

        // Create stub blob to track the upload
        Map<String, String> metadata = new HashMap<>();
        metadata.put(TARGET_BLOB_NAME_KEY, targetBlobName);
        metadata.put(UPLOAD_ID_KEY, uploadId);

        // Copy user metadata if present
        var userMetadata = blobMetadata.getUserMetadata();
        if (userMetadata != null) {
            for (var entry : userMetadata.entrySet()) {
                metadata.put("user_" + entry.getKey(), entry.getValue());
            }
        }

        // Store content metadata
        var contentMeta = blobMetadata.getContentMetadata();
        if (contentMeta != null) {
            if (contentMeta.getContentType() != null) {
                metadata.put("content_type", contentMeta.getContentType());
            }
            if (contentMeta.getCacheControl() != null) {
                metadata.put("cache_control", contentMeta.getCacheControl());
            }
            if (contentMeta.getContentDisposition() != null) {
                metadata.put("content_disposition",
                        contentMeta.getContentDisposition());
            }
            if (contentMeta.getContentEncoding() != null) {
                metadata.put("content_encoding",
                        contentMeta.getContentEncoding());
            }
            if (contentMeta.getContentLanguage() != null) {
                metadata.put("content_language",
                        contentMeta.getContentLanguage());
            }
        }

        // Store tier if not standard
        if (blobMetadata.getTier() != null &&
                blobMetadata.getTier() != Tier.STANDARD) {
            metadata.put("storage_tier", blobMetadata.getTier().name());
        }

        BlobInfo stubInfo = BlobInfo.newBuilder(container, stubKey)
                .setMetadata(metadata)
                .build();

        try {
            storage.create(stubInfo, new byte[0]);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, stubKey);
            throw se;
        }

        return MultipartUpload.create(container, targetBlobName,
                uploadId, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        String container = mpu.containerName();
        String uploadId = mpu.id();

        // Delete stub blob
        String stubKey = STUB_BLOB_PREFIX + uploadId;
        storage.delete(BlobId.of(container, stubKey));

        // Delete all part blobs
        String partPrefix = MULTIPART_PREFIX + uploadId + "/";
        var page = storage.list(container, BlobListOption.prefix(partPrefix));
        for (Blob blob : page.iterateAll()) {
            storage.delete(blob.getBlobId());
        }
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu,
            List<MultipartPart> parts) {
        String container = mpu.containerName();
        String uploadId = mpu.id();
        String stubKey = STUB_BLOB_PREFIX + uploadId;

        // Get stub blob for metadata
        Blob stubBlob = storage.get(BlobId.of(container, stubKey));
        if (stubBlob == null) {
            throw new IllegalArgumentException(
                    "Upload not found: uploadId=" + uploadId);
        }

        Map<String, String> stubMeta = stubBlob.getMetadata();
        if (stubMeta == null) {
            throw new IllegalArgumentException(
                    "Stub blob metadata is null: uploadId=" + uploadId);
        }

        String targetBlobName = stubMeta.get(TARGET_BLOB_NAME_KEY);
        if (targetBlobName == null) {
            throw new IllegalArgumentException(
                    "Stub blob missing target name: uploadId=" + uploadId);
        }

        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("Parts list cannot be empty");
        }

        // Validate parts are in order
        int previousPartNumber = 0;
        for (var part : parts) {
            if (part.partNumber() <= previousPartNumber) {
                throw new IllegalArgumentException(
                        "Parts must be in strictly ascending order");
            }
            previousPartNumber = part.partNumber();
        }

        // Build list of source blobs for compose
        List<String> sourceKeys = new ArrayList<>();
        for (var part : parts) {
            String partKey = MULTIPART_PREFIX + uploadId + "/" + part.partNumber();
            Blob partBlob = storage.get(BlobId.of(container, partKey));
            if (partBlob == null) {
                throw new IllegalArgumentException(
                        "Part " + part.partNumber() + " not found");
            }
            sourceKeys.add(partKey);
        }

        // Restore metadata from stub (already captured stubMeta above)
        Map<String, String> userMetadata = new HashMap<>();
        String contentType = null;
        String cacheControl = null;
        String contentDisposition = null;
        String contentEncoding = null;
        String contentLanguage = null;
        com.google.cloud.storage.StorageClass storageClass = null;

        for (var entry : stubMeta.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("user_")) {
                userMetadata.put(key.substring(5), value);
            } else if ("content_type".equals(key)) {
                contentType = value;
            } else if ("cache_control".equals(key)) {
                cacheControl = value;
            } else if ("content_disposition".equals(key)) {
                contentDisposition = value;
            } else if ("content_encoding".equals(key)) {
                contentEncoding = value;
            } else if ("content_language".equals(key)) {
                contentLanguage = value;
            } else if ("storage_tier".equals(key)) {
                storageClass = toStorageClass(Tier.valueOf(value));
            }
        }

        // Compose parts into final blob
        // GCS compose limit is 32, so we may need to compose in stages
        String resultEtag = composeBlobs(container, targetBlobName, sourceKeys,
                contentType, cacheControl, contentDisposition,
                contentEncoding, contentLanguage, userMetadata, storageClass,
                mpu.putOptions());

        // Cleanup: delete stub and part blobs
        storage.delete(BlobId.of(container, stubKey));
        for (String partKey : sourceKeys) {
            storage.delete(BlobId.of(container, partKey));
        }

        return resultEtag;
    }

    private String composeBlobs(String container, String targetKey,
            List<String> sourceKeys, String contentType, String cacheControl,
            String contentDisposition, String contentEncoding,
            String contentLanguage, Map<String, String> userMetadata,
            com.google.cloud.storage.StorageClass storageClass,
            PutOptions options) {

        // Handle conditional writes
        List<BlobTargetOption> targetOptions = new ArrayList<>();
        if (options instanceof PutOptions2) {
            var putOptions2 = (PutOptions2) options;
            if (putOptions2.getIfMatch() != null) {
                targetOptions.add(BlobTargetOption.generationMatch(
                        parseGeneration(putOptions2.getIfMatch())));
            }
            if (putOptions2.getIfNoneMatch() != null) {
                if ("*".equals(putOptions2.getIfNoneMatch())) {
                    targetOptions.add(BlobTargetOption.doesNotExist());
                } else {
                    targetOptions.add(BlobTargetOption.generationNotMatch(
                            parseGeneration(putOptions2.getIfNoneMatch())));
                }
            }
        }

        // If we have more than 32 sources, compose in stages
        while (sourceKeys.size() > GCS_COMPOSE_LIMIT) {
            List<String> newSourceKeys = new ArrayList<>();
            for (int i = 0; i < sourceKeys.size(); i += GCS_COMPOSE_LIMIT) {
                int end = Math.min(i + GCS_COMPOSE_LIMIT, sourceKeys.size());
                List<String> batch = sourceKeys.subList(i, end);

                String intermediateKey = MULTIPART_PREFIX +
                        UUID.randomUUID().toString() + "/intermediate";
                composeSubset(container, intermediateKey, batch);
                newSourceKeys.add(intermediateKey);
            }
            // Clean up the original sources
            for (String key : sourceKeys) {
                storage.delete(BlobId.of(container, key));
            }
            sourceKeys = newSourceKeys;
        }

        // Final compose
        BlobInfo.Builder targetInfo = BlobInfo.newBuilder(container, targetKey);
        if (contentType != null) {
            targetInfo.setContentType(contentType);
        }
        if (cacheControl != null) {
            targetInfo.setCacheControl(cacheControl);
        }
        if (contentDisposition != null) {
            targetInfo.setContentDisposition(contentDisposition);
        }
        if (contentEncoding != null) {
            targetInfo.setContentEncoding(contentEncoding);
        }
        if (contentLanguage != null) {
            targetInfo.setContentLanguage(contentLanguage);
        }
        if (userMetadata != null && !userMetadata.isEmpty()) {
            targetInfo.setMetadata(userMetadata);
        }
        if (storageClass != null) {
            targetInfo.setStorageClass(storageClass);
        }

        ComposeRequest.Builder composeRequest = ComposeRequest.newBuilder()
                .setTarget(targetInfo.build());

        for (String sourceKey : sourceKeys) {
            composeRequest.addSource(sourceKey);
        }

        if (!targetOptions.isEmpty()) {
            composeRequest.setTargetOptions(
                    targetOptions.toArray(new BlobTargetOption[0]));
        }

        try {
            Blob result = storage.compose(composeRequest.build());
            return result.getEtag();
        } catch (StorageException se) {
            translateAndRethrowException(se, container, targetKey);
            throw se;
        }
    }

    private void composeSubset(String container, String targetKey,
            List<String> sourceKeys) {
        BlobInfo targetInfo = BlobInfo.newBuilder(container, targetKey).build();
        ComposeRequest.Builder composeRequest = ComposeRequest.newBuilder()
                .setTarget(targetInfo);
        for (String sourceKey : sourceKeys) {
            composeRequest.addSource(sourceKey);
        }
        storage.compose(composeRequest.build());
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu,
            int partNumber, Payload payload) {
        if (partNumber < 1 || partNumber > MAX_PARTS) {
            throw new IllegalArgumentException(
                    "Part number must be between 1 and " + MAX_PARTS +
                            ", got: " + partNumber);
        }

        Long contentLength = payload.getContentMetadata().getContentLength();
        if (contentLength == null) {
            throw new IllegalArgumentException("Content-Length is required");
        }
        if (contentLength < 0) {
            throw new IllegalArgumentException(
                    "Content-Length must be non-negative, got: " + contentLength);
        }
        if (contentLength > getMaximumMultipartPartSize()) {
            throw new IllegalArgumentException(
                    "Part size exceeds maximum of " +
                            getMaximumMultipartPartSize() +
                            " bytes: " + contentLength);
        }

        String container = mpu.containerName();
        String uploadId = mpu.id();
        String partKey = MULTIPART_PREFIX + uploadId + "/" + partNumber;

        byte[] md5Hash;
        try (var is = payload.openStream();
             var his = new HashingInputStream(MD5, is)) {
            byte[] content = his.readAllBytes();
            md5Hash = his.hash().asBytes();

            // Verify Content-MD5 if provided
            var providedMd5 = payload.getContentMetadata()
                    .getContentMD5AsHashCode();
            if (providedMd5 != null) {
                if (!MessageDigest.isEqual(md5Hash, providedMd5.asBytes())) {
                    throw new IllegalArgumentException("Content-MD5 mismatch");
                }
            }

            BlobInfo partInfo = BlobInfo.newBuilder(container, partKey)
                    .setMd5(Base64.getEncoder().encodeToString(md5Hash))
                    .build();
            storage.create(partInfo, content);

        } catch (StorageException se) {
            translateAndRethrowException(se, container, partKey);
            throw new RuntimeException("Failed to upload part " + partNumber, se);
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to upload part " + partNumber, ioe);
        }

        String eTag = BaseEncoding.base16().lowerCase().encode(md5Hash);
        return MultipartPart.create(partNumber, contentLength, eTag, null);
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        String container = mpu.containerName();
        String uploadId = mpu.id();
        String partPrefix = MULTIPART_PREFIX + uploadId + "/";

        var parts = ImmutableList.<MultipartPart>builder();
        var page = storage.list(container, BlobListOption.prefix(partPrefix));

        for (Blob blob : page.iterateAll()) {
            String name = blob.getName();
            String partNumberStr = name.substring(partPrefix.length());
            try {
                int partNumber = Integer.parseInt(partNumberStr);
                String eTag = blob.getMd5() != null ?
                        BaseEncoding.base16().lowerCase().encode(
                                Base64.getDecoder().decode(blob.getMd5())) : "";
                parts.add(MultipartPart.create(partNumber, blob.getSize(),
                        eTag, toDate(blob.getUpdateTimeOffsetDateTime())));
            } catch (NumberFormatException e) {
                // Skip non-part blobs
            }
        }

        return parts.build();
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {
        var uploads = ImmutableList.<MultipartUpload>builder();
        var page = storage.list(container,
                BlobListOption.prefix(STUB_BLOB_PREFIX));

        for (Blob blob : page.iterateAll()) {
            Map<String, String> meta = blob.getMetadata();
            if (meta == null) {
                continue;
            }
            String targetBlobName = meta.get(TARGET_BLOB_NAME_KEY);
            String uploadId = meta.get(UPLOAD_ID_KEY);
            if (targetBlobName != null && uploadId != null) {
                uploads.add(MultipartUpload.create(container, targetBlobName,
                        uploadId, null, null));
            }
        }

        return uploads.build();
    }

    @Override
    public long getMinimumMultipartPartSize() {
        // GCS compose has no minimum part size
        return 1;
    }

    @Override
    public long getMaximumMultipartPartSize() {
        // 5 GB per object for standard uploads
        return 5L * 1024 * 1024 * 1024;
    }

    @Override
    public int getMaximumNumberOfParts() {
        return MAX_PARTS;
    }

    @Override
    public InputStream streamBlob(String container, String name) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    // ========== Helper Methods ==========

    private static Date toDate(java.time.OffsetDateTime offsetDateTime) {
        if (offsetDateTime == null) {
            return null;
        }
        return Date.from(offsetDateTime.toInstant());
    }

    private static Tier toTier(
            com.google.cloud.storage.StorageClass storageClass) {
        if (storageClass == null) {
            return Tier.STANDARD;
        }
        if (storageClass.equals(
                com.google.cloud.storage.StorageClass.STANDARD)) {
            return Tier.STANDARD;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.NEARLINE)) {
            return Tier.INFREQUENT;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.COLDLINE)) {
            return Tier.COOL;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.ARCHIVE)) {
            return Tier.ARCHIVE;
        } else {
            return Tier.STANDARD;
        }
    }

    private static com.google.cloud.storage.StorageClass toStorageClass(
            Tier tier) {
        if (tier == null) {
            return com.google.cloud.storage.StorageClass.STANDARD;
        }
        return switch (tier) {
        case STANDARD -> com.google.cloud.storage.StorageClass.STANDARD;
        case INFREQUENT -> com.google.cloud.storage.StorageClass.NEARLINE;
        case COOL -> com.google.cloud.storage.StorageClass.COLDLINE;
        case COLD -> com.google.cloud.storage.StorageClass.COLDLINE;
        case ARCHIVE -> com.google.cloud.storage.StorageClass.ARCHIVE;
        };
    }

    private static ContentMetadata toContentMetadata(Blob blob) {
        Long size = blob.getSize();
        return ContentMetadataBuilder.create()
                .cacheControl(blob.getCacheControl())
                .contentDisposition(blob.getContentDisposition())
                .contentEncoding(blob.getContentEncoding())
                .contentLanguage(blob.getContentLanguage())
                .contentLength(size != null ? size : 0L)
                .contentType(blob.getContentType())
                .build();
    }

    /**
     * Parse generation number from ETag.
     * GCS ETags for objects contain the generation number.
     */
    private static long parseGeneration(String etag) {
        if (etag == null) {
            return 0;
        }
        // Strip quotes if present
        etag = etag.replace("\"", "");
        try {
            return Long.parseLong(etag);
        } catch (NumberFormatException e) {
            // If not a number, try to extract from base64
            return 0;
        }
    }

    /**
     * Translate GCS StorageException to jclouds exceptions.
     */
    private void translateAndRethrowException(StorageException se,
            String container, @Nullable String key) {
        int code = se.getCode();
        if (code == 404) {
            if (key != null) {
                var exception = new KeyNotFoundException(container, key, "");
                exception.initCause(se);
                throw exception;
            } else {
                var exception = new ContainerNotFoundException(container, "");
                exception.initCause(se);
                throw exception;
            }
        } else if (code == 412) {
            var request = HttpRequest.builder()
                    .method("GET")
                    .endpoint("https://storage.googleapis.com")
                    .build();
            var response = HttpResponse.builder()
                    .statusCode(Status.PRECONDITION_FAILED.getStatusCode())
                    .build();
            throw new HttpResponseException(
                    new HttpCommand(request), response, se);
        } else if (code == 409) {
            var request = HttpRequest.builder()
                    .method("PUT")
                    .endpoint("https://storage.googleapis.com")
                    .build();
            var response = HttpResponse.builder()
                    .statusCode(Status.CONFLICT.getStatusCode())
                    .build();
            throw new HttpResponseException(
                    new HttpCommand(request), response, se);
        } else if (code == 400) {
            var request = HttpRequest.builder()
                    .method("GET")
                    .endpoint("https://storage.googleapis.com")
                    .build();
            var response = HttpResponse.builder()
                    .statusCode(Status.BAD_REQUEST.getStatusCode())
                    .build();
            throw new HttpResponseException(
                    new HttpCommand(request), response, se);
        }
    }
}
