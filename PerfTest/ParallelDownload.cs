
//using Microsoft.WindowsAzure.Storage.Core.Util;
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
    public class ParallelDownload
    {
        public readonly ParallelDownloadSettings ParallelDownloadSettings;

        public Task Task { get; private set; }

        //private int parallelIOCount;

        //private long chunkSize;

        private long? offset;

        private long? length;

        //private string path;

        //private FileMode fileMode;

        List<Stream> viewStreams;
        //List<Tuple<Task, MemoryMappedViewStream, long, long>> viewStreamTasks;
        List<ParallelDownloadTask> parallelDownloadTasks;

        List<Task> downloadTaskList;

        private List<Task> continuationTasks = new List<Task>();

        private ParallelDownload(ParallelDownloadSettings parallelDownloadSettings)
        {
            this.ParallelDownloadSettings = parallelDownloadSettings;
            this.viewStreams = new List<Stream>();
            //this.viewStreamTasks = new List<Tuple<Task, MemoryMappedViewStream, long, long>>();
            this.parallelDownloadTasks = new List<ParallelDownloadTask>();
            this.offset = parallelDownloadSettings.Offset;
            this.length = parallelDownloadSettings.Length;
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings)
        {
            return Start(parallelDownloadSettings, null, null, null, CancellationToken.None);
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            ParallelDownload parallelDownload = new ParallelDownload(parallelDownloadSettings);
            parallelDownload.Task = StartAsync(parallelDownload, accessConditions, operationContext, options, cancellationToken);

            return parallelDownload;
        }

        private static async Task StartAsync(ParallelDownload parallelDownload, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            options = new BlobRequestOptions();
            //options.MaximumExecutionTime = TimeSpan.FromMinutes(1);
            ParallelDownloadSettings downloadSettings = parallelDownload.ParallelDownloadSettings;
            long timeStamp = Stopwatch.GetTimestamp();
            CloudBlob blob = downloadSettings.Blob;
            if (!parallelDownload.length.HasValue)
            {
                if (blob.Properties.Length == -1)
                {
                    await blob.FetchAttributesAsync(accessConditions, options, operationContext);
                }

                parallelDownload.length = blob.Properties.Length;
            }

            if (!downloadSettings.Offset.HasValue)
            {
                parallelDownload.offset = 0;
            }

            parallelDownload.downloadTaskList = new List<Task>();
            int totalIOReadCalls = (int)Math.Ceiling((double)parallelDownload.length.Value / (double)downloadSettings.MaxRangeSize);
            Console.WriteLine($"TotalIOreadCalls={totalIOReadCalls}");
            try
            {
                //using (var fs = new FileStream(downloadSettings.FilePath, downloadSettings.FileMode, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan))
                using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(downloadSettings.FilePath, downloadSettings.FileMode, null, parallelDownload.length.Value/*, MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, false*/))
                {
                    for (int i = 0; i < totalIOReadCalls; i++)
                    {
                        Console.WriteLine($"About to get block={i}");
                        if (parallelDownload.downloadTaskList.Count >= downloadSettings.ParallelIOCount)
                        {
                            // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                            Console.WriteLine("Hit max I/Os, waiting for 1 to complete");
                            Task downloadChunk = await Task.WhenAny(parallelDownload.downloadTaskList).ConfigureAwait(false);
                            try
                            {
                                await downloadChunk;
                                Console.WriteLine("An I/O completed");
                                parallelDownload.downloadTaskList.Remove(downloadChunk);
                                Console.WriteLine("Removed pending I/O");
                                ParallelDownloadTask currentParallelDownloadTask = parallelDownload.parallelDownloadTasks.Find(t => t.DownloadTask.Id == downloadChunk.Id);
                                currentParallelDownloadTask.ViewStream.Dispose();
                            }
                            catch (Exception ex)
                            {
                                if ((ex.GetType().Equals(typeof(StorageException)) && ex.InnerException.GetType().Equals(typeof(TimeoutException))) ||
                                    ex.GetType().Equals(typeof(IOException)) || ex.GetType().Equals(typeof(WebException)))
                                {
                                    Console.WriteLine("Hit timeout exception and going to retry");
                                    ParallelDownloadTask taskToRetry = parallelDownload.parallelDownloadTasks.Find(t => t.DownloadTask.Id == downloadChunk.Id);
                                    parallelDownload.parallelDownloadTasks.Remove(taskToRetry);
                                    Console.WriteLine("range to retry:{0}-{1}", taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                    taskToRetry.ViewStream.Seek(0, SeekOrigin.Begin);
                                    parallelDownload.downloadTaskList.Remove(downloadChunk);
                                    downloadChunk = downloadSettings.Blob.DownloadRangeToStreamAsync(taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength, accessConditions, options, operationContext, cancellationToken);
                                    parallelDownload.downloadTaskList.Add(downloadChunk);
                                    taskToRetry = new ParallelDownloadTask(downloadChunk, taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                    parallelDownload.parallelDownloadTasks.Add(taskToRetry);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }

                        long streamBeginIndex = parallelDownload.offset.Value + (i * downloadSettings.MaxRangeSize);
                        long streamReadSize = downloadSettings.MaxRangeSize;
                        // last range may be smaller than the chunk size
                        if (i == totalIOReadCalls - 1)
                        {
                            streamReadSize = parallelDownload.length.Value - (parallelDownload.offset.Value + (i * streamReadSize));
                            Console.WriteLine("About to request the last block");
                        }

                        Console.WriteLine($"Creating MMF stream for block{i}, offset={streamBeginIndex}, size={streamReadSize}");
                        MemoryMappedViewStream viewStream = mmf.CreateViewStream(streamBeginIndex, streamReadSize);
                        //var viewStream = System.IO.Stream.Null;//..MemoryStream(new Byte[streamReadSize]);//.CreateViewStream(streamBeginIndex, streamReadSize);

                        parallelDownload.viewStreams.Add(viewStream);
                        Console.WriteLine($"Initiating download of block={i}");
                        Task downloadTask = downloadSettings.Blob.DownloadRangeToStreamAsync(viewStream, streamBeginIndex, streamReadSize, accessConditions, options, operationContext, cancellationToken);
                        parallelDownload.downloadTaskList.Add(downloadTask);
                        Console.WriteLine($"Add block={i} task to collection, outstanding I/O={parallelDownload.downloadTaskList.Count}");
                        ParallelDownloadTask parallelDownloadTask = new ParallelDownloadTask(downloadTask, viewStream, streamBeginIndex, streamReadSize);
                        parallelDownload.parallelDownloadTasks.Add(parallelDownloadTask);
                    }

                    Console.WriteLine($"for loop done; about to wait for all remaining I/Os={parallelDownload.downloadTaskList.Count}");
                    while (parallelDownload.downloadTaskList.Count > 0)
                    {
                        // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                        Console.WriteLine($"Outside for loop waiting for remaining downloadTask count={parallelDownload.downloadTaskList.Count}");
                        Task downloadChunk = await Task.WhenAny(parallelDownload.downloadTaskList).ConfigureAwait(false);
                        try
                        {
                            await downloadChunk;
                            Console.WriteLine("An I/O completed");
                            parallelDownload.downloadTaskList.Remove(downloadChunk);
                            Console.WriteLine("Removed pending I/O");
                            ParallelDownloadTask currentParallelDownloadTask = parallelDownload.parallelDownloadTasks.Find(t => t.DownloadTask.Id == downloadChunk.Id);
                            currentParallelDownloadTask.ViewStream.Dispose();
                        }
                        catch (Exception ex)
                        {
                            if ((ex.GetType().Equals(typeof(StorageException)) && ex.InnerException.GetType().Equals(typeof(TimeoutException))))// ||
                                //ex.GetType().Equals(typeof(IOException)) || ex.GetType().Equals(typeof(WebException)))
                            {
                                Console.WriteLine("Hit timeout exception and going to retry");
                                ParallelDownloadTask taskToRetry = parallelDownload.parallelDownloadTasks.Find(t => t.DownloadTask.Id == downloadChunk.Id);
                                parallelDownload.parallelDownloadTasks.Remove(taskToRetry);
                                Console.WriteLine("range to retry:{0}-{1}", taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                taskToRetry.ViewStream.Seek(0, SeekOrigin.Begin);
                                parallelDownload.downloadTaskList.Remove(downloadChunk);
                                downloadChunk = downloadSettings.Blob.DownloadRangeToStreamAsync(taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength, accessConditions, options, operationContext, cancellationToken);
                                parallelDownload.downloadTaskList.Add(downloadChunk);
                                taskToRetry = new ParallelDownloadTask(downloadChunk, taskToRetry.ViewStream, taskToRetry.BlobOffset, taskToRetry.RangeLength);
                                parallelDownload.parallelDownloadTasks.Add(taskToRetry);
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }

                    //Console.WriteLine($"for loop done; about to wait for all remaining I/Os={parallelDownload.downloadTaskList.Count}");
                    //await Task.WhenAll(parallelDownload.downloadTaskList).ConfigureAwait(false);
                    Console.WriteLine("All I/Os complete");
                }
            }
            finally
            {
                Console.WriteLine($"In finally, disposing {parallelDownload.viewStreams.Count} streams");
                foreach (var viewStream in parallelDownload.viewStreams)
                {
                    viewStream.Dispose();
                }
                Console.WriteLine("leaving finally");
            }
        }
    }
}
