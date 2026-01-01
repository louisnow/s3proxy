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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Response.Status;

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
import org.jclouds.io.ContentMetadataBuilder;
import org.jclouds.io.Payload;
import org.jclouds.io.PayloadSlicer;
import org.jclouds.providers.ProviderMetadata;

@Singleton
public final class GcsBlobStore extends BaseBlobStore {
    private static final String STUB_BLOB_PREFIX = ".s3proxy-mpu/";
    private static final String TARGET_BLOB_NAME_META = "s3proxy-target-blob";
    private static final int GCS_MAX_COMPOSE_OBJECTS = 32;

    private final Storage storage;
    private final String projectId;

    @Inject
    GcsBlobStore(BlobStoreContext context, BlobUtils blobUtils,
            Supplier<Location> defaultLocation,
            @Memoized Supplier<Set<? extends Location>> locations,
            PayloadSlicer slicer,
            @org.jclouds.location.Provider Supplier<Credentials> creds,
            ProviderMetadata provider,
            @Named(GcsBlobApiMetadata.GCS_PROJECT_ID) String projectId) {
        super(context, blobUtils, defaultLocation, locations, slicer);
        this.projectId = projectId;

        var cred = creds.get();
        StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();

        if (!projectId.isEmpty()) {
            optionsBuilder.setProjectId(projectId);
        }

        if (!cred.identity.isEmpty() && !cred.credential.isEmpty()) {
            try {
                String jsonKey = String.format(
                    "{\"type\":\"service_account\"," +
                    "\"client_email\":\"%s\"," +
                    "\"private_key\":\"%s\"," +
                    "\"project_id\":\"%s\"}",
                    cred.identity,
                    cred.credential.replace("\n", "\\n"),
                    projectId);
                GoogleCredentials credentials = ServiceAccountCredentials
                        .fromStream(new ByteArrayInputStream(
                                jsonKey.getBytes()));
                optionsBuilder.setCredentials(credentials);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to create GCS credentials", e);
            }
        }

        storage = optionsBuilder.build().getService();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        var set = ImmutableSet.<StorageMetadata>builder();
        for (var bucket : storage.list().iterateAll()) {
            set.add(new StorageMetadataImpl(StorageType.CONTAINER, null,
                    bucket.getName(), null, null,
                    null, toDate(bucket.getCreateTimeOffsetDateTime()),
                    toDate(bucket.getUpdateTimeOffsetDateTime()),
                    Map.of(), null, Tier.STANDARD));
        }
        return new PageSetImpl<StorageMetadata>(set.build(), null);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container,
            ListContainerOptions options) {
        var gcsOptions = Storage.BlobListOption.prefix(
                options.getPrefix() != null ? options.getPrefix() : "");

        var listOptions = new java.util.ArrayList<Storage.BlobListOption>();
        listOptions.add(gcsOptions);

        if (options.getMaxResults() != null) {
            listOptions.add(Storage.BlobListOption.pageSize(
                    options.getMaxResults()));
        }

        if (options.getMarker() != null) {
            listOptions.add(Storage.BlobListOption.pageToken(
                    options.getMarker()));
        }

        if (options.getDelimiter() != null) {
            listOptions.add(Storage.BlobListOption.delimiter(
                    options.getDelimiter()));
        }

        var set = ImmutableSet.<StorageMetadata>builder();
        com.google.api.gax.paging.Page<Blob> page;
        try {
            page = storage.list(container,
                    listOptions.toArray(new Storage.BlobListOption[0]));
        } catch (StorageException se) {
            translateAndRethrowException(se, container, null);
            throw se;
        }

        for (var blob : page.getValues()) {
            if (blob.isDirectory()) {
                set.add(new StorageMetadataImpl(StorageType.RELATIVE_PATH,
                        null, blob.getName(), null,
                        null, null, null, null,
                        Map.of(), null, Tier.STANDARD));
            } else {
                set.add(new StorageMetadataImpl(StorageType.BLOB,
                        null, blob.getName(), null,
                        null, blob.getEtag(),
                        toDate(blob.getCreateTimeOffsetDateTime()),
                        toDate(blob.getUpdateTimeOffsetDateTime()),
                        Map.of(), blob.getSize(),
                        toTier(blob.getStorageClass())));
            }
        }

        return new PageSetImpl<StorageMetadata>(set.build(),
                page.getNextPageToken());
    }

    @Override
    public boolean containerExists(String container) {
        return storage.get(container) != null;
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
            var bucketInfo = BucketInfo.newBuilder(container).build();
            storage.create(bucketInfo);
            return true;
        } catch (StorageException se) {
            if (se.getCode() == 409) {
                return false;
            }
            throw se;
        }
    }

    @Override
    public void deleteContainer(String container) {
        try {
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
            var page = storage.list(container, Storage.BlobListOption.pageSize(1));
            if (page.getValues().iterator().hasNext()) {
                return false;
            }
            return storage.delete(container);
        } catch (StorageException se) {
            if (se.getCode() == 404) {
                return true;
            }
            throw se;
        }
    }

    @Override
    public boolean blobExists(String container, String key) {
        return storage.get(BlobId.of(container, key)) != null;
    }

    @Override
    public org.jclouds.blobstore.domain.Blob getBlob(String container,
            String key, GetOptions options) {
        var blobId = BlobId.of(container, key);
        Blob gcsBlob;
        try {
            gcsBlob = storage.get(blobId);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, key);
            throw se;
        }

        if (gcsBlob == null) {
            return null;
        }

        long offset = 0;
        Long length = null;
        if (!options.getRanges().isEmpty()) {
            var ranges = options.getRanges().get(0).split("-", 2);

            if (ranges[0].isEmpty()) {
                throw new UnsupportedOperationException(
                        "trailing ranges unsupported");
            } else if (ranges[1].isEmpty()) {
                offset = Long.parseLong(ranges[0]);
            } else {
                offset = Long.parseLong(ranges[0]);
                long end = Long.parseLong(ranges[1]);
                length = end - offset + 1;
            }
        }

        ReadChannel reader = storage.reader(blobId);
        if (offset > 0) {
            try {
                reader.seek(offset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (length != null) {
            reader.limit(offset + length);
        }
        InputStream inputStream = Channels.newInputStream(reader);

        long contentLength;
        if (length != null) {
            contentLength = length;
        } else {
            contentLength = gcsBlob.getSize() - offset;
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

        if (offset > 0 || length != null) {
            blob.getAllHeaders().put(HttpHeaders.CONTENT_RANGE,
                    "bytes " + offset +
                    "-" + (offset + contentLength - 1) +
                    "/" + gcsBlob.getSize());
        }

        var metadata = blob.getMetadata();
        metadata.setETag(gcsBlob.getEtag());
        metadata.setCreationDate(toDate(gcsBlob.getCreateTimeOffsetDateTime()));
        metadata.setLastModified(toDate(gcsBlob.getUpdateTimeOffsetDateTime()));
        if (gcsBlob.getMd5() != null) {
            metadata.getContentMetadata().setContentMD5(
                    HashCode.fromBytes(
                            java.util.Base64.getDecoder().decode(
                                    gcsBlob.getMd5())));
        }
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
        var blobId = BlobId.of(container, blob.getMetadata().getName());
        var contentMetadata = blob.getMetadata().getContentMetadata();

        var blobInfoBuilder = BlobInfo.newBuilder(blobId);
        blobInfoBuilder.setContentType(contentMetadata.getContentType());
        blobInfoBuilder.setContentDisposition(
                contentMetadata.getContentDisposition());
        blobInfoBuilder.setContentEncoding(contentMetadata.getContentEncoding());
        blobInfoBuilder.setContentLanguage(contentMetadata.getContentLanguage());
        blobInfoBuilder.setCacheControl(contentMetadata.getCacheControl());

        if (blob.getMetadata().getUserMetadata() != null) {
            blobInfoBuilder.setMetadata(blob.getMetadata().getUserMetadata());
        }

        var hash = contentMetadata.getContentMD5AsHashCode();
        if (hash != null) {
            blobInfoBuilder.setMd5(
                    java.util.Base64.getEncoder().encodeToString(
                            hash.asBytes()));
        }

        if (blob.getMetadata().getTier() != Tier.STANDARD) {
            blobInfoBuilder.setStorageClass(
                    toStorageClass(blob.getMetadata().getTier()));
        }

        try (var is = blob.getPayload().openStream()) {
            byte[] content = ByteStreams.toByteArray(is);
            Blob created = storage.create(blobInfoBuilder.build(), content);
            return created.getEtag();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, null);
            throw se;
        }
    }

    @Override
    public String copyBlob(String fromContainer, String fromName,
            String toContainer, String toName, CopyOptions options) {
        var source = BlobId.of(fromContainer, fromName);
        var target = BlobId.of(toContainer, toName);

        var copyRequest = Storage.CopyRequest.newBuilder()
                .setSource(source)
                .setTarget(target)
                .build();

        try {
            var result = storage.copy(copyRequest);
            return result.getResult().getEtag();
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
        var blob = storage.get(BlobId.of(container, key));
        if (blob == null) {
            return null;
        }

        var contentMetadataBuilder = ContentMetadataBuilder.create()
                .cacheControl(blob.getCacheControl())
                .contentDisposition(blob.getContentDisposition())
                .contentEncoding(blob.getContentEncoding())
                .contentLanguage(blob.getContentLanguage())
                .contentLength(blob.getSize())
                .contentType(blob.getContentType());

        if (blob.getMd5() != null) {
            contentMetadataBuilder.contentMD5(HashCode.fromBytes(
                    java.util.Base64.getDecoder().decode(blob.getMd5())));
        }

        return new BlobMetadataImpl(
                null,
                key,
                null,
                null,
                blob.getEtag(),
                toDate(blob.getCreateTimeOffsetDateTime()),
                toDate(blob.getUpdateTimeOffsetDateTime()),
                blob.getMetadata() != null ? blob.getMetadata() : Map.of(),
                null,
                container,
                contentMetadataBuilder.build(),
                blob.getSize(),
                toTier(blob.getStorageClass()));
    }

    @Override
    protected boolean deleteAndVerifyContainerGone(String container) {
        storage.delete(container);
        return true;
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        var bucket = storage.get(container);
        if (bucket == null) {
            throw new ContainerNotFoundException(container, "");
        }
        return ContainerAccess.PRIVATE;
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess access) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public BlobAccess getBlobAccess(String container, String key) {
        return BlobAccess.PRIVATE;
    }

    @Override
    public void setBlobAccess(String container, String key, BlobAccess access) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container,
            BlobMetadata blobMetadata, PutOptions options) {
        if (!containerExists(container)) {
            throw new ContainerNotFoundException(container, "");
        }

        String uploadId = UUID.randomUUID().toString();
        String stubBlobName = STUB_BLOB_PREFIX + uploadId;

        var metadata = new java.util.HashMap<String, String>();
        metadata.put(TARGET_BLOB_NAME_META, blobMetadata.getName());
        if (blobMetadata.getUserMetadata() != null) {
            metadata.putAll(blobMetadata.getUserMetadata());
        }

        var contentMetadata = blobMetadata.getContentMetadata();
        var blobInfoBuilder = BlobInfo.newBuilder(
                BlobId.of(container, stubBlobName));
        blobInfoBuilder.setMetadata(metadata);
        if (contentMetadata != null) {
            blobInfoBuilder.setContentType(contentMetadata.getContentType());
            blobInfoBuilder.setContentDisposition(
                    contentMetadata.getContentDisposition());
            blobInfoBuilder.setContentEncoding(
                    contentMetadata.getContentEncoding());
            blobInfoBuilder.setContentLanguage(
                    contentMetadata.getContentLanguage());
            blobInfoBuilder.setCacheControl(contentMetadata.getCacheControl());
        }

        storage.create(blobInfoBuilder.build(), new byte[0]);

        return MultipartUpload.create(container, blobMetadata.getName(),
                uploadId, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        String container = mpu.containerName();
        String uploadId = mpu.id();

        var parts = listMultipartUpload(mpu);
        for (var part : parts) {
            String partBlobName = getPartBlobName(uploadId, part.partNumber());
            try {
                storage.delete(BlobId.of(container, partBlobName));
            } catch (StorageException se) {
                if (se.getCode() != 404) {
                    throw se;
                }
            }
        }

        String stubBlobName = STUB_BLOB_PREFIX + uploadId;
        try {
            storage.delete(BlobId.of(container, stubBlobName));
        } catch (StorageException se) {
            if (se.getCode() != 404) {
                throw se;
            }
        }
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu,
            List<MultipartPart> parts) {
        String container = mpu.containerName();
        String uploadId = mpu.id();
        String targetBlobName = mpu.blobName();
        String stubBlobName = STUB_BLOB_PREFIX + uploadId;

        Blob stubBlob = storage.get(BlobId.of(container, stubBlobName));
        if (stubBlob == null) {
            throw new IllegalArgumentException(
                    "Upload not found: uploadId=" + uploadId);
        }

        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("Parts list cannot be empty");
        }

        int previousPartNumber = 0;
        for (var part : parts) {
            int partNumber = part.partNumber();
            if (partNumber <= previousPartNumber) {
                throw new IllegalArgumentException(
                        "Parts must be in strictly ascending order");
            }
            previousPartNumber = partNumber;
        }

        List<BlobId> partBlobIds = new ArrayList<>();
        for (var part : parts) {
            String partBlobName = getPartBlobName(uploadId, part.partNumber());
            Blob partBlob = storage.get(BlobId.of(container, partBlobName));
            if (partBlob == null) {
                throw new IllegalArgumentException(
                        "Part " + part.partNumber() + " not found");
            }
            partBlobIds.add(partBlob.getBlobId());
        }

        var stubMetadata = stubBlob.getMetadata();
        var targetBlobInfoBuilder = BlobInfo.newBuilder(
                BlobId.of(container, targetBlobName));
        targetBlobInfoBuilder.setContentType(stubBlob.getContentType());
        targetBlobInfoBuilder.setContentDisposition(
                stubBlob.getContentDisposition());
        targetBlobInfoBuilder.setContentEncoding(stubBlob.getContentEncoding());
        targetBlobInfoBuilder.setContentLanguage(stubBlob.getContentLanguage());
        targetBlobInfoBuilder.setCacheControl(stubBlob.getCacheControl());

        if (stubMetadata != null) {
            var userMetadata = new java.util.HashMap<>(stubMetadata);
            userMetadata.remove(TARGET_BLOB_NAME_META);
            if (!userMetadata.isEmpty()) {
                targetBlobInfoBuilder.setMetadata(userMetadata);
            }
        }

        Blob finalBlob = composeWithRecursion(container, targetBlobInfoBuilder,
                partBlobIds);

        for (BlobId partBlobId : partBlobIds) {
            try {
                storage.delete(partBlobId);
            } catch (StorageException se) {
                if (se.getCode() != 404) {
                    throw se;
                }
            }
        }

        try {
            storage.delete(BlobId.of(container, stubBlobName));
        } catch (StorageException se) {
            if (se.getCode() != 404) {
                throw se;
            }
        }

        return finalBlob.getEtag();
    }

    private Blob composeWithRecursion(String container,
            BlobInfo.Builder targetBuilder, List<BlobId> blobIds) {
        if (blobIds.size() <= GCS_MAX_COMPOSE_OBJECTS) {
            var composeRequest = Storage.ComposeRequest.newBuilder()
                    .setTarget(targetBuilder.build())
                    .addSource(blobIds.stream()
                            .map(id -> id.getName())
                            .toList())
                    .build();
            return storage.compose(composeRequest);
        }

        List<BlobId> intermediateBlobIds = new ArrayList<>();
        int batchIndex = 0;

        for (int i = 0; i < blobIds.size(); i += GCS_MAX_COMPOSE_OBJECTS) {
            int end = Math.min(i + GCS_MAX_COMPOSE_OBJECTS, blobIds.size());
            List<BlobId> batch = blobIds.subList(i, end);

            String intermediateName = STUB_BLOB_PREFIX + "compose-" +
                    UUID.randomUUID() + "-" + batchIndex;
            var intermediateBuilder = BlobInfo.newBuilder(
                    BlobId.of(container, intermediateName));

            var composeRequest = Storage.ComposeRequest.newBuilder()
                    .setTarget(intermediateBuilder.build())
                    .addSource(batch.stream()
                            .map(id -> id.getName())
                            .toList())
                    .build();
            Blob intermediateBlob = storage.compose(composeRequest);
            intermediateBlobIds.add(intermediateBlob.getBlobId());
            batchIndex++;
        }

        Blob result = composeWithRecursion(container, targetBuilder,
                intermediateBlobIds);

        for (BlobId intermediateId : intermediateBlobIds) {
            try {
                storage.delete(intermediateId);
            } catch (StorageException se) {
                if (se.getCode() != 404) {
                    throw se;
                }
            }
        }

        return result;
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu,
            int partNumber, Payload payload) {
        if (partNumber < 1 || partNumber > 10000) {
            throw new IllegalArgumentException(
                    "Part number must be between 1 and 10,000, got: " +
                    partNumber);
        }

        String container = mpu.containerName();
        String uploadId = mpu.id();
        String partBlobName = getPartBlobName(uploadId, partNumber);

        Long contentLength = payload.getContentMetadata().getContentLength();
        if (contentLength == null) {
            throw new IllegalArgumentException("Content-Length is required");
        }

        var blobInfoBuilder = BlobInfo.newBuilder(
                BlobId.of(container, partBlobName));

        try (var is = payload.openStream()) {
            byte[] content = ByteStreams.toByteArray(is);
            Blob partBlob = storage.create(blobInfoBuilder.build(), content);

            String eTag = partBlob.getEtag();
            Date lastModified = toDate(partBlob.getUpdateTimeOffsetDateTime());
            return MultipartPart.create(partNumber, contentLength, eTag,
                    lastModified);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (StorageException se) {
            translateAndRethrowException(se, container, partBlobName);
            throw se;
        }
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        String container = mpu.containerName();
        String uploadId = mpu.id();
        String partPrefix = getPartBlobName(uploadId, 0);
        partPrefix = partPrefix.substring(0, partPrefix.lastIndexOf("_") + 1);

        var parts = new ArrayList<MultipartPart>();
        var page = storage.list(container,
                Storage.BlobListOption.prefix(partPrefix));

        for (var blob : page.iterateAll()) {
            String name = blob.getName();
            String partNumberStr = name.substring(name.lastIndexOf("_") + 1);
            try {
                int partNumber = Integer.parseInt(partNumberStr);
                parts.add(MultipartPart.create(partNumber, blob.getSize(),
                        blob.getEtag(),
                        toDate(blob.getUpdateTimeOffsetDateTime())));
            } catch (NumberFormatException e) {
                continue;
            }
        }

        parts.sort((a, b) -> Integer.compare(a.partNumber(), b.partNumber()));
        return ImmutableList.copyOf(parts);
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {
        var uploads = new ArrayList<MultipartUpload>();
        var page = storage.list(container,
                Storage.BlobListOption.prefix(STUB_BLOB_PREFIX));

        for (var blob : page.iterateAll()) {
            String name = blob.getName();
            if (name.contains("/compose-")) {
                continue;
            }
            String uploadId = name.substring(STUB_BLOB_PREFIX.length());
            var metadata = blob.getMetadata();
            String targetBlobName = metadata != null ?
                    metadata.get(TARGET_BLOB_NAME_META) : null;
            if (targetBlobName != null) {
                uploads.add(MultipartUpload.create(container, targetBlobName,
                        uploadId, null, null));
            }
        }

        return ImmutableList.copyOf(uploads);
    }

    private static String getPartBlobName(String uploadId, int partNumber) {
        return String.format("%s%s_%08d", STUB_BLOB_PREFIX, uploadId,
                partNumber);
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return 5 * 1024 * 1024;
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return 5L * 1024 * 1024 * 1024;
    }

    @Override
    public int getMaximumNumberOfParts() {
        return 10000;
    }

    @Override
    public InputStream streamBlob(String container, String name) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static Date toDate(@Nullable OffsetDateTime time) {
        if (time == null) {
            return null;
        }
        return new Date(time.toInstant().toEpochMilli());
    }

    private static com.google.cloud.storage.StorageClass toStorageClass(
            Tier tier) {
        return switch (tier) {
        case ARCHIVE -> com.google.cloud.storage.StorageClass.ARCHIVE;
        case COLD -> com.google.cloud.storage.StorageClass.COLDLINE;
        case COOL -> com.google.cloud.storage.StorageClass.NEARLINE;
        case INFREQUENT -> com.google.cloud.storage.StorageClass.NEARLINE;
        case STANDARD -> com.google.cloud.storage.StorageClass.STANDARD;
        };
    }

    private static Tier toTier(
            @Nullable com.google.cloud.storage.StorageClass storageClass) {
        if (storageClass == null) {
            return Tier.STANDARD;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.ARCHIVE)) {
            return Tier.ARCHIVE;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.COLDLINE)) {
            return Tier.COLD;
        } else if (storageClass.equals(
                com.google.cloud.storage.StorageClass.NEARLINE)) {
            return Tier.COOL;
        } else {
            return Tier.STANDARD;
        }
    }

    private void translateAndRethrowException(StorageException se,
            String container, @Nullable String key) {
        if (se.getCode() == 404) {
            if (key != null) {
                var exception = new KeyNotFoundException(container, key, "");
                exception.initCause(se);
                throw exception;
            } else {
                var exception = new ContainerNotFoundException(container, "");
                exception.initCause(se);
                throw exception;
            }
        } else if (se.getCode() == 412) {
            var request = HttpRequest.builder()
                    .method("GET")
                    .endpoint("https://storage.googleapis.com")
                    .build();
            var response = HttpResponse.builder()
                    .statusCode(Status.PRECONDITION_FAILED.getStatusCode())
                    .build();
            throw new HttpResponseException(
                    new HttpCommand(request), response, se);
        }
    }
}
