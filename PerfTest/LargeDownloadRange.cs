using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Blob
{
    internal class LargeDownloadRange
    {
        public Task DownloadTask { get; set; }

        public LargeDownloadStream LargeDownloadStream { get; protected set; }

        public long BlobOffset { get; protected set; }

        public long RangeLength { get; protected set; }

        public LargeDownloadRange(Task downloadTask, LargeDownloadStream largeDownloadStream, long blobOffSet, long rangeLength)
        {
            this.DownloadTask = downloadTask;
            this.LargeDownloadStream = largeDownloadStream;
            this.BlobOffset = blobOffSet;
            this.RangeLength = rangeLength;
        }
    }
}
