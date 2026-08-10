#include "cache/origin/s3/S3ObjectStore.h"

#ifdef HF3FS_ENABLE_CACHE

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#if __has_include(<aws/core/client/AWSAuthSigner.h>)
#include <aws/core/client/AWSAuthSigner.h>
#else
#include <aws/core/auth/AWSAuthSigner.h>
#endif
#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <iterator>

namespace hf3fs::cache::origin::s3 {
namespace {

class AwsRuntime {
 public:
  AwsRuntime() { Aws::InitAPI(options_); }
  ~AwsRuntime() { Aws::ShutdownAPI(options_); }

 private:
  Aws::SDKOptions options_;
};

AwsRuntime &runtime() {
  static AwsRuntime instance;
  return instance;
}

template <typename Error>
S3Failure mapAwsFailure(const Error &error) {
  auto status = static_cast<int>(error.GetResponseCode());
  auto name = std::string(error.GetExceptionName().c_str());
  auto message = std::string(error.GetMessage().c_str());
  S3FailureKind kind = S3FailureKind::OTHER;
  if (name.find("NoSuchUpload") != std::string::npos) {
    kind = S3FailureKind::NO_SUCH_UPLOAD;
  } else if (status == 404) {
    kind = S3FailureKind::NOT_FOUND;
  } else if (status == 412) {
    kind = S3FailureKind::VERSION_MISMATCH;
  } else if (status == 401 || status == 403) {
    kind = S3FailureKind::AUTHENTICATION;
  } else if (status == 408) {
    kind = S3FailureKind::TIMEOUT;
  } else if (status == 429) {
    kind = S3FailureKind::THROTTLED;
  } else if (status >= 500 && status < 600) {
    kind = S3FailureKind::SERVER;
  } else if (name.find("Timeout") != std::string::npos || name.find("NETWORK_CONNECTION") != std::string::npos) {
    kind = S3FailureKind::TIMEOUT;
  }
  return S3Failure{kind, status, std::move(message)};
}

class AwsS3RequestExecutor final : public S3RequestExecutor {
 public:
  explicit AwsS3RequestExecutor(const AwsS3ClientConfig &config) {
    (void)runtime();
    Aws::Client::ClientConfiguration clientConfig;
    if (!config.region.empty()) clientConfig.region = config.region.c_str();
    if (!config.endpoint.empty()) clientConfig.endpointOverride = config.endpoint.c_str();
    clientConfig.scheme = config.useTls ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    clientConfig.maxConnections = config.maxConnections;
    clientConfig.connectTimeoutMs = static_cast<long>(config.connectTimeout.asMs().count());
    clientConfig.requestTimeoutMs = static_cast<long>(config.requestTimeout.asMs().count());

    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials;
    if (config.accessKey.empty() && config.secretKey.empty()) {
      credentials = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    } else {
      credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(config.accessKey.c_str(),
                                                                              config.secretKey.c_str(),
                                                                              config.sessionToken.c_str());
    }
    client_ = std::make_unique<Aws::S3::S3Client>(credentials,
                                                  clientConfig,
                                                  Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                  !config.pathStyle);
  }

  S3Outcome<HeadResponse> head(const HeadRequest &request) override {
    Aws::S3::Model::HeadObjectRequest awsRequest;
    awsRequest.SetBucket(request.object.bucket.c_str());
    awsRequest.SetKey(request.object.key.c_str());
    auto outcome = client_->HeadObject(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    auto &result = outcome.GetResult();
    if (result.GetContentLength() < 0) {
      return S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "S3 HEAD returned a negative Content-Length"};
    }
    HeadResponse response;
    response.size = static_cast<uint64_t>(result.GetContentLength());
    if (!result.GetVersionId().empty()) response.versionId = result.GetVersionId().c_str();
    if (!result.GetETag().empty()) response.etag = result.GetETag().c_str();
    return response;
  }

  S3Outcome<GetRangeResponse> getRange(const GetRangeRequest &request) override {
    Aws::S3::Model::GetObjectRequest awsRequest;
    awsRequest.SetBucket(request.object.bucket.c_str());
    awsRequest.SetKey(request.object.key.c_str());
    awsRequest.SetRange(
        fmt::format("bytes={}-{}", request.range.offset, request.range.offset + request.range.length - 1).c_str());
    if (request.object.version.type == VersionSelectorType::VERSION_ID) {
      awsRequest.SetVersionId(request.object.version.value.c_str());
    } else {
      awsRequest.SetIfMatch(fmt::format("\"{}\"", request.object.version.value).c_str());
    }
    auto outcome = client_->GetObject(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    auto result = outcome.GetResultWithOwnership();
    GetRangeResponse response;
    response.httpStatus = 206;
    response.contentRange = result.GetContentRange().c_str();
    if (!result.GetVersionId().empty()) response.versionId = result.GetVersionId().c_str();
    if (!result.GetETag().empty()) response.etag = result.GetETag().c_str();
    auto &stream = result.GetBody();
    response.body.assign(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
    return response;
  }

  S3Outcome<ListResponse> listObjects(const ListRequest &request) override {
    Aws::S3::Model::ListObjectsV2Request awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetPrefix(request.prefix.c_str());
    awsRequest.SetMaxKeys(static_cast<int>(request.maxKeys));
    if (!request.continuation.empty()) awsRequest.SetContinuationToken(request.continuation.c_str());
    auto outcome = client_->ListObjectsV2(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    const auto &result = outcome.GetResult();
    ListResponse response;
    response.truncated = result.GetIsTruncated();
    response.nextContinuation = result.GetNextContinuationToken().c_str();
    response.objects.reserve(result.GetContents().size());
    for (const auto &object : result.GetContents()) {
      if (object.GetSize() < 0) {
        return S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "S3 LIST returned a negative object size"};
      }
      ListedObject listed;
      listed.key = object.GetKey().c_str();
      listed.size = static_cast<uint64_t>(object.GetSize());
      if (!object.GetETag().empty()) listed.etag = object.GetETag().c_str();
      response.objects.push_back(std::move(listed));
    }
    return response;
  }

  S3Outcome<S3CreateMultipartResponse> createMultipartUpload(const S3CreateMultipartRequest &request) override {
    Aws::S3::Model::CreateMultipartUploadRequest awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetKey(request.key.c_str());
    auto outcome = client_->CreateMultipartUpload(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    S3CreateMultipartResponse response;
    response.uploadId = outcome.GetResult().GetUploadId().c_str();
    if (response.uploadId.empty()) {
      return S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "S3 multipart create returned an empty upload id"};
    }
    return response;
  }

  S3Outcome<S3UploadPartResponse> uploadPart(const S3UploadPartRequest &request) override {
    Aws::S3::Model::UploadPartRequest awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetKey(request.key.c_str());
    awsRequest.SetUploadId(request.uploadId.c_str());
    awsRequest.SetPartNumber(static_cast<int>(request.partNumber));
    awsRequest.SetContentLength(static_cast<long long>(request.body.size()));
    auto body = Aws::MakeShared<Aws::StringStream>("hf3fs-cache-multipart-part");
    if (!request.body.empty()) {
      body->write(reinterpret_cast<const char *>(request.body.data()),
                  static_cast<std::streamsize>(request.body.size()));
    }
    awsRequest.SetBody(std::move(body));
    auto outcome = client_->UploadPart(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    S3UploadPartResponse response;
    response.etag = outcome.GetResult().GetETag().c_str();
    if (response.etag.empty()) {
      return S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "S3 part upload returned an empty ETag"};
    }
    return response;
  }

  S3Outcome<S3CompleteMultipartResponse> completeMultipartUpload(const S3CompleteMultipartRequest &request) override {
    Aws::S3::Model::CompletedMultipartUpload completed;
    for (const auto &part : request.parts) {
      Aws::S3::Model::CompletedPart awsPart;
      awsPart.SetPartNumber(static_cast<int>(part.partNumber));
      auto etag = part.etag;
      if (!(etag.size() >= 2 && etag.front() == '"' && etag.back() == '"')) etag = fmt::format("\"{}\"", etag);
      awsPart.SetETag(etag.c_str());
      completed.AddParts(std::move(awsPart));
    }
    Aws::S3::Model::CompleteMultipartUploadRequest awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetKey(request.key.c_str());
    awsRequest.SetUploadId(request.uploadId.c_str());
    awsRequest.SetMultipartUpload(std::move(completed));
    auto outcome = client_->CompleteMultipartUpload(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    const auto &result = outcome.GetResult();
    S3CompleteMultipartResponse response;
    response.bucket = result.GetBucket().c_str();
    response.key = result.GetKey().c_str();
    if (!result.GetVersionId().empty()) response.versionId = result.GetVersionId().c_str();
    if (!result.GetETag().empty()) response.etag = result.GetETag().c_str();
    if (response.bucket.empty() || response.key.empty()) {
      return S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "S3 multipart completion omitted object identity"};
    }
    return response;
  }

  S3Outcome<S3AbortMultipartResponse> abortMultipartUpload(const S3AbortMultipartRequest &request) override {
    Aws::S3::Model::AbortMultipartUploadRequest awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetKey(request.key.c_str());
    awsRequest.SetUploadId(request.uploadId.c_str());
    auto outcome = client_->AbortMultipartUpload(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    return S3AbortMultipartResponse{};
  }

  S3Outcome<S3DeleteObjectResponse> deleteObject(const S3DeleteObjectRequest &request) override {
    Aws::S3::Model::DeleteObjectRequest awsRequest;
    awsRequest.SetBucket(request.bucket.c_str());
    awsRequest.SetKey(request.key.c_str());
    if (request.versionId) awsRequest.SetVersionId(request.versionId->c_str());
    auto outcome = client_->DeleteObject(awsRequest);
    if (!outcome.IsSuccess()) return mapAwsFailure(outcome.GetError());
    return S3DeleteObjectResponse{};
  }

 private:
  std::unique_ptr<Aws::S3::S3Client> client_;
};

}  // namespace

Result<std::shared_ptr<S3ObjectStore>> S3ObjectStore::createAws(S3ObjectStoreConfig config,
                                                                const AwsS3ClientConfig &clientConfig) {
  if (clientConfig.accessKey.empty() != clientConfig.secretKey.empty()) {
    return makeError(StatusCode::kInvalidConfig, "S3 access key and secret key must be configured together");
  }
  if (!clientConfig.sessionToken.empty() && clientConfig.accessKey.empty()) {
    return makeError(StatusCode::kInvalidConfig, "S3 session token requires static credentials");
  }
  return std::make_shared<S3ObjectStore>(std::move(config), std::make_unique<AwsS3RequestExecutor>(clientConfig));
}

}  // namespace hf3fs::cache::origin::s3

#else

namespace hf3fs::cache::origin::s3 {

Result<std::shared_ptr<S3ObjectStore>> S3ObjectStore::createAws(S3ObjectStoreConfig, const AwsS3ClientConfig &) {
  return makeError(CacheCode::kFeatureDisabled, "3FS was built without the AWS SDK S3 backend");
}

}  // namespace hf3fs::cache::origin::s3

#endif
