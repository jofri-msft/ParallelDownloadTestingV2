
using Microsoft.WindowsAzure.Storage.Core.Util;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using System.IO;
namespace Microsoft.WindowsAzure.Storage.Blob
{
    /// <summary>
    /// The settings for downloading a blob in parallel.
    /// </summary>
    public sealed class ParallelDownloadSettings
    {
        public CloudBlob Blob {get; private set;}
        public readonly string FilePath;
        public readonly FileMode FileMode;
        public readonly int ParallelIOCount;
        public readonly long MaxRangeSize;
        public readonly long? Offset;
        public readonly long? Length;

        public ParallelDownloadSettings(CloudBlob blob, string filePath, FileMode fileMode, long? offset, long? length, int parallelIOCount, long maxRangeSize)
        {
            //CommonUtility.AssertNotNull("blob", blob);
            //CommonUtility.AssertNotNull("filePath", filePath);
            //CommonUtility.AssertInBounds("parallelIOCount", parallelIOCount, 1, Constants.MaxParallelOperationThreadCount);
            this.Blob = blob;
            this.FilePath = filePath;
            this.FileMode = fileMode;
            this.ParallelIOCount = parallelIOCount;
            this.MaxRangeSize = maxRangeSize;
            this.Offset = offset;
            this.Length = length;
        }
/*
        public ParallelDownloadSettings(CloudBlob blob, string filePath, FileMode fileMode)
        {
            this.Blob = blob;
            this.FilePath = filePath;
            this.FileMode = fileMode;
            this.ParallelIOCount = Constants.DefaultParallelIOCount;
            this.MaxRangeSize = Constants.DefaultReadWriteBlockSizeBytes;
        }*/
    }
}
