
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

        private long? Offset;

        private long? Length;

        List<Stream> viewStreams;
        List<LargeDownloadSubtask> largeDownloadSubtasks;

        List<Task> downloadTaskList;

        private List<Task> continuationTasks = new List<Task>();

        private LargeDownloadToFile(LargeDownloadToFileSettings largeDownloadToFileSettings)
        {
            this.LargeDownloadToFileSettings = largeDownloadToFileSettings;
            this.viewStreams = new List<Stream>();
            this.largeDownloadSubtasks = new List<LargeDownloadSubtask>();
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
            parallelDownload.Task = StartAsync(parallelDownload, accessConditions, operationContext, cancellationToken);

            return parallelDownload;
        }

        private static async Task StartAsync(LargeDownloadToFile largeDownloadToFile, AccessCondition accessConditions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            LargeDownloadToFileSettings downloadSettings = largeDownloadToFile.LargeDownloadToFileSettings;
            BlobRequestOptions options = new BlobRequestOptions();
            options.MaximumExecutionTime = TimeSpan.FromMinutes(1);
            options.RetryPolicy = downloadSettings.RetryPolicy;
            options.EncryptionPolicy = downloadSettings.EncryptionPolicy;
            options.RequireEncryption = downloadSettings.RequireEncryption;
            options.LocationMode = downloadSettings.LocationMode;

            CloudBlob blob = downloadSettings.Blob;

            // Always do a head request to have an ETag to lock-on to.
            // This code is designed for only large blobs so this request should be neglibile on perf
            await blob.FetchAttributesAsync(accessConditions, options, operationContext);
            if (accessConditions == null)
            {
                accessConditions = new AccessCondition();
            }

            accessConditions.IfMatchETag = blob.Properties.ETag;
            if (!largeDownloadToFile.Length.HasValue)
            {
                largeDownloadToFile.Length = blob.Properties.Length;
            }

            if (!downloadSettings.Offset.HasValue)
            {
                largeDownloadToFile.Offset = 0;
            }

            largeDownloadToFile.downloadTaskList = new List<Task>();
            int totalIOReadCalls = (int)Math.Ceiling((double)largeDownloadToFile.Length.Value / (double)downloadSettings.MaxRangeSizeInBytes);
            //Console.WriteLine($"TotalIOreadCalls={totalIOReadCalls}");
            try
            {
                using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(downloadSettings.FilePath, downloadSettings.FileMode, null, largeDownloadToFile.Length.Value))
                {
                    for (int i = 0; i < totalIOReadCalls; i++)
                    {
                        if (largeDownloadToFile.downloadTaskList.Count >= downloadSettings.ParallelIOCount)
                        {
                            // The number of on-going I/O operations has reached its maximum, wait until one completes or has an error.
                            Task downloadRangeTask = await Task.WhenAny(largeDownloadToFile.downloadTaskList).ConfigureAwait(false);
                            try
                            {
                                // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                                await downloadRangeTask;
                                largeDownloadToFile.downloadTaskList.Remove(downloadRangeTask);
                                LargeDownloadSubtask currentParallelDownloadTask = largeDownloadToFile.largeDownloadSubtasks.Find(t => t.DownloadTask.Id == downloadRangeTask.Id);
                                currentParallelDownloadTask.ViewStream.Dispose();
                            }
                            catch (Exception ex)
                            {
                                if (ex.GetType().Equals(typeof(StorageException)) && ex.InnerException.GetType().Equals(typeof(TimeoutException)))
                                {
                                    //Console.WriteLine("Hit timeout exception and going to retry");
                                    LargeDownloadSubtask taskToRetry = largeDownloadToFile.largeDownloadSubtasks.Find(t => t.DownloadTask.Id == downloadRangeTask.Id);
                                    largeDownloadToFile.largeDownloadSubtasks.Remove(taskToRetry);
                                    //Console.WriteLine("range to retry:{0}-{1} at position:{2}", taskToRetry.BlobOffset, taskToRetry.RangeLength, taskToRetry.ViewStream.Position);
                                    //taskToRetry.ViewStream.Seek(0, SeekOrigin.Begin);
                                    largeDownloadToFile.downloadTaskList.Remove(downloadRangeTask);
                                    downloadRangeTask = downloadSettings.Blob.DownloadRangeToStreamAsync(taskToRetry.ViewStream, taskToRetry.BlobOffset + taskToRetry.ViewStream.Position, taskToRetry.RangeLength - taskToRetry.ViewStream.Position, accessConditions, options, operationContext, cancellationToken);
                                    largeDownloadToFile.downloadTaskList.Add(downloadRangeTask);
                                    taskToRetry = new LargeDownloadSubtask(downloadRangeTask, taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                    largeDownloadToFile.largeDownloadSubtasks.Add(taskToRetry);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }

                        long streamBeginIndex = largeDownloadToFile.Offset.Value + (i * downloadSettings.MaxRangeSizeInBytes);
                        long streamReadSize = downloadSettings.MaxRangeSizeInBytes;
                        // last range may be smaller than the range size
                        if (i == totalIOReadCalls - 1)
                        {
                            streamReadSize = largeDownloadToFile.Length.Value - (largeDownloadToFile.Offset.Value + (i * streamReadSize));
                        }

                        //Console.WriteLine($"Creating MMF stream for block{i}, offset={streamBeginIndex}, size={streamReadSize}");
                        MemoryMappedViewStream viewStream = mmf.CreateViewStream(streamBeginIndex, streamReadSize);
                        largeDownloadToFile.viewStreams.Add(viewStream);
                        Task downloadTask = downloadSettings.Blob.DownloadRangeToStreamAsync(viewStream, streamBeginIndex, streamReadSize, accessConditions, options, operationContext, cancellationToken);
                        largeDownloadToFile.downloadTaskList.Add(downloadTask);
                        LargeDownloadSubtask largeDownloadSubtask = new LargeDownloadSubtask(downloadTask, viewStream, streamBeginIndex, streamReadSize);
                        largeDownloadToFile.largeDownloadSubtasks.Add(largeDownloadSubtask);
                    }

                    while (largeDownloadToFile.downloadTaskList.Count > 0)
                    {
                        // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                        //Console.WriteLine($"Outside for loop waiting for remaining downloadTask count={largeDownloadToFile.downloadTaskList.Count}");
                        Task downloadRangeTask = await Task.WhenAny(largeDownloadToFile.downloadTaskList).ConfigureAwait(false);
                        try
                        {
                            await downloadRangeTask;
                            largeDownloadToFile.downloadTaskList.Remove(downloadRangeTask);
                            LargeDownloadSubtask currentDownloadSubtask = largeDownloadToFile.largeDownloadSubtasks.Find(t => t.DownloadTask.Id == downloadRangeTask.Id);
                            currentDownloadSubtask.ViewStream.Dispose();
                        }
                        catch (Exception ex)
                        {
                            if ((ex.GetType().Equals(typeof(StorageException)) && ex.InnerException.GetType().Equals(typeof(TimeoutException))))// ||
                                //ex.GetType().Equals(typeof(IOException)) || ex.GetType().Equals(typeof(WebException)))
                            {
                                //Console.WriteLine("Hit timeout exception and going to retry");
                                LargeDownloadSubtask taskToRetry = largeDownloadToFile.largeDownloadSubtasks.Find(t => t.DownloadTask.Id == downloadRangeTask.Id);
                                largeDownloadToFile.largeDownloadSubtasks.Remove(taskToRetry);
                                //Console.WriteLine("range to retry:{0}-{1} at postion:{2}", taskToRetry.BlobOffset, taskToRetry.RangeLength, taskToRetry.ViewStream.Position);
                                //taskToRetry.ViewStream.Seek(0, SeekOrigin.Begin);
                                largeDownloadToFile.downloadTaskList.Remove(downloadRangeTask);
                                downloadRangeTask = downloadSettings.Blob.DownloadRangeToStreamAsync(taskToRetry.ViewStream, taskToRetry.BlobOffset + taskToRetry.ViewStream.Position, taskToRetry.RangeLength - taskToRetry.ViewStream.Position, accessConditions, options, operationContext, cancellationToken);
                                largeDownloadToFile.downloadTaskList.Add(downloadRangeTask);
                                taskToRetry = new LargeDownloadSubtask(downloadRangeTask, taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                largeDownloadToFile.largeDownloadSubtasks.Add(taskToRetry);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                }
            }
            finally
            {
                foreach (var viewStream in largeDownloadToFile.viewStreams)
                {
                    viewStream.Dispose();
                }
            }
        }
    }
}
