using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Blob
{
    class ParallelDownloadTask
    {
        public Task DownloadTask { get; set; }

        public MemoryMappedViewStream ViewStream { get; protected set; }

        public long BlobOffset { get; protected set; }

        public long RangeLength { get; protected set; }

        public ParallelDownloadTask(Task downloadTask, MemoryMappedViewStream viewStream, long blobOffSet, long rangeLength)
        {
            this.DownloadTask = downloadTask;
            this.ViewStream = viewStream;
            this.BlobOffset = blobOffSet;
            this.RangeLength = rangeLength;
        }
    }
}
