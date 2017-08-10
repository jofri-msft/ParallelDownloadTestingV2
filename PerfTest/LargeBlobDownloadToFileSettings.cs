
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.IO;
namespace Microsoft.WindowsAzure.Storage.Blob
{
    /// <summary>
    /// The settings for downloading a large blob to a file.
    /// </summary>
    public sealed class LargeDownloadToFileSettings
    {
        public CloudBlob Blob {get; private set;}
        public readonly string FilePath;
        public readonly FileMode FileMode;
        public readonly long? Offset;
        public readonly long? Length;
        public readonly int ParallelIOCount;
        public readonly long MaxRangeSizeInBytes;
        public readonly TimeSpan? MaxExecutionTimePerRange;
        public readonly IRetryPolicy RetryPolicy;
        public readonly BlobEncryptionPolicy EncryptionPolicy;
        public readonly bool? RequireEncryption;
        public readonly LocationMode? LocationMode;

        public LargeDownloadToFileSettings(CloudBlob blob, string filePath, FileMode fileMode, long? offset, long? length, int parallelIOCount = 16,
            long maxRangeSizeInBytes = 100*1024*1024, TimeSpan? maxExecutionTimePerRange = null, IRetryPolicy retryPolicy = null, BlobEncryptionPolicy encryptionPolicy = null, bool? requireEncryption = null,
            LocationMode? locationMode = null)
        {
            //CommonUtility.AssertNotNull("blob", blob);
            //CommonUtility.AssertNotNull("filePath", filePath);
            //CommonUtility.AssertInBounds("parallelIOCount", parallelIOCount, 1, int.MaxValue);

            this.Blob = blob;
            this.FilePath = filePath;
            this.FileMode = fileMode;
            this.Offset = offset;
            this.Length = length;
            this.ParallelIOCount = parallelIOCount;
            this.MaxRangeSizeInBytes = maxRangeSizeInBytes;
            this.MaxExecutionTimePerRange = maxExecutionTimePerRange;
            this.RetryPolicy = retryPolicy;
            this.EncryptionPolicy = encryptionPolicy;
            this.RequireEncryption = requireEncryption;
            this.LocationMode = locationMode;
        }

    }
}
