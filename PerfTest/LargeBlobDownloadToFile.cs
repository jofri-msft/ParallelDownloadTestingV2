
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net;

namespace Microsoft.WindowsAzure.Storage.Blob
{
    public class LargeDownloadToFile
    {
        public readonly LargeDownloadToFileSettings LargeDownloadToFileSettings;

        public Task Task { get; private set; }

        /// <summary>
        /// The starting offset in the blob to start downloading
        /// </summary>
        private long? Offset;

        private long? Length;

        /// <summary>
        /// The list of underlying tasks
        /// </summary>
        private List<Task> downloadTaskList;

        /// <summary>
        /// The user's cancellation token.
        /// </summary>
        //protected CancellationToken cancellationToken;
        protected BlobRequestOptions blobRequestOptions;
        protected OperationContext operationContext;
        protected AccessCondition accessConditions;

        private LargeDownloadToFile(LargeDownloadToFileSettings largeDownloadToFileSettings)
        {
            this.LargeDownloadToFileSettings = largeDownloadToFileSettings;
            this.downloadTaskList = new List<Task>();

            this.Offset = largeDownloadToFileSettings.Offset;
            this.Length = largeDownloadToFileSettings.Length;
        }

        public static LargeDownloadToFile Start(LargeDownloadToFileSettings largeDownloadToFileSettings)
        {
            return Start(largeDownloadToFileSettings, null, null, CancellationToken.None);
        }

        public static LargeDownloadToFile Start(LargeDownloadToFileSettings parallelDownloadSettings, AccessCondition accessConditions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            LargeDownloadToFile parallelDownload = new LargeDownloadToFile(parallelDownloadSettings);
            parallelDownload.Task = parallelDownload.StartAsync(accessConditions, operationContext, cancellationToken);

            return parallelDownload;
        }

        private async Task StartAsync(AccessCondition accessConditions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            //LargeDownloadToFileSettings downloadSettings = largeDownloadToFile.LargeDownloadToFileSettings;
            BlobRequestOptions blobRequestOptions = new BlobRequestOptions();
            //blobRequestOptions.MaximumExecutionTime = TimeSpan.FromMinutes(1);
            blobRequestOptions.RetryPolicy = this.LargeDownloadToFileSettings.RetryPolicy;
            blobRequestOptions.EncryptionPolicy = this.LargeDownloadToFileSettings.EncryptionPolicy;
            blobRequestOptions.RequireEncryption = this.LargeDownloadToFileSettings.RequireEncryption;
            blobRequestOptions.LocationMode = this.LargeDownloadToFileSettings.LocationMode;

            // these properties need to be preserved 
            this.operationContext = operationContext;
            this.blobRequestOptions = blobRequestOptions;
            this.accessConditions = accessConditions;

            CloudBlob blob = this.LargeDownloadToFileSettings.Blob;

            // Always do a head request to have an ETag to lock-on to.
            // This code is designed for only large blobs so this request should be neglibile on perf
            await blob.FetchAttributesAsync(this.accessConditions, this.blobRequestOptions, this.operationContext);
            if (this.accessConditions == null)
            {
                this.accessConditions = new AccessCondition();
            }

            this.accessConditions.IfMatchETag = blob.Properties.ETag;
            if (!this.Length.HasValue)
            {
                this.Length = blob.Properties.Length;
            }

            if (!this.LargeDownloadToFileSettings.Offset.HasValue)
            {
                this.Offset = 0;
            }

            int totalIOReadCalls = (int)Math.Ceiling((double)this.Length.Value / (double)this.LargeDownloadToFileSettings.MaxRangeSizeInBytes);

            using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(this.LargeDownloadToFileSettings.FilePath, this.LargeDownloadToFileSettings.FileMode, null, this.Length.Value))
            {
                for (int i = 0; i < totalIOReadCalls; i++)
                {
                    if (this.downloadTaskList.Count >= this.LargeDownloadToFileSettings.ParallelIOCount)
                    {
                        // The number of on-going I/O operations has reached its maximum, wait until one completes or has an error.
                        Task downloadRangeTask = await Task.WhenAny(this.downloadTaskList).ConfigureAwait(false);

                        // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                        await downloadRangeTask;

                        this.downloadTaskList.Remove(downloadRangeTask);
                    }

                    long streamBeginIndex = this.Offset.Value + (i * this.LargeDownloadToFileSettings.MaxRangeSizeInBytes);
                    long streamReadSize = this.LargeDownloadToFileSettings.MaxRangeSizeInBytes;
                    // last range may be smaller than the range size
                    if (i == totalIOReadCalls - 1)
                    {
                        streamReadSize = this.Length.Value - (this.Offset.Value + (i * streamReadSize));
                    }

                    MemoryMappedViewStream viewStream = mmf.CreateViewStream(streamBeginIndex, streamReadSize);
                    Task downloadTask = this.downloadToStreamWrapper(viewStream, streamBeginIndex, streamReadSize, this.accessConditions, this.blobRequestOptions, this.operationContext, cancellationToken);
                    this.downloadTaskList.Add(downloadTask);
                }

                while (this.downloadTaskList.Count > 0)
                {
                    // The number of on-going I/O operations has reached its maximum, wait until one completes or has an error.
                    Task downloadRangeTask = await Task.WhenAny(this.downloadTaskList).ConfigureAwait(false);

                    // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                    await downloadRangeTask;

                    this.downloadTaskList.Remove(downloadRangeTask);
                }
            }
        }

        /// <summary>
        /// Wraps the downloadToStream logic to retry/recover the download operation
        /// in the case that the last time the input stream has been written to exceeds a threshold.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="blobOffset"></param>
        /// <param name="length"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task downloadToStreamWrapper(UnmanagedMemoryStream stream, long blobOffset, long length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            long startingOffset = blobOffset;
            LargeDownloadStream largeDownloadStream = null;
            while (true)
            {
                try
                {
                    largeDownloadStream = new LargeDownloadStream(stream, blobOffset);
                    CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(largeDownloadStream.CancellationToken, cancellationToken);
                    Task downloadTask = this.LargeDownloadToFileSettings.Blob.DownloadRangeToStreamAsync(largeDownloadStream, blobOffset, length, this.accessConditions, this.blobRequestOptions, this.operationContext, cts.Token);
                    await downloadTask;
                    largeDownloadStream.Close();
                    break;
                }
                catch (OperationCanceledException)
                // only catch if the stream triggered the cancellation
                when (!cancellationToken.IsCancellationRequested)
                {
                    blobOffset = startingOffset + largeDownloadStream.Position;
                    largeDownloadStream.Close();
                    length -= (blobOffset - startingOffset);
                    Console.WriteLine($"cancellation for offset:{startingOffset}, now at position:{blobOffset}. With remaining length:{length}");
                    if (length == 0)
                    {
                        break;
                    }
                }
            }

            stream.Dispose();
        }
    }
}
